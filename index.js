require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { Firestore } = require('@google-cloud/firestore');
const { google } = require('googleapis');
const { createCanvas } = require('canvas');
const { Storage } = require('@google-cloud/storage');
const OpenAI = require('openai');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { DateTime } = require('luxon');

const app = express();
app.use(express.json());

// ─────────────────────────────────────────────
// CLIENTS
// ─────────────────────────────────────────────

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const serviceAccount = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);

const firestore = new Firestore({
  projectId: serviceAccount.project_id,
  credentials: {
    client_email: serviceAccount.client_email,
    private_key: serviceAccount.private_key,
  },
});

const storage = new Storage({
  projectId: serviceAccount.project_id,
  credentials: serviceAccount,
});

const googleAuth = new google.auth.GoogleAuth({
  credentials: serviceAccount,
  scopes: ['https://www.googleapis.com/auth/drive'],
});

const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const ADMIN_MESSENGER_ID = process.env.ADMIN_MESSENGER_ID;
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;
const QUEUE_TASK_TOKEN = process.env.QUEUE_TASK_TOKEN;

// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────

// Shared courts: Basketball + Pickleball share A, B, C
// Booking a court blocks BOTH sports for that slot
const SHARED_COURTS = ['A', 'B', 'C'];
const SPORTS = {
  BASKETBALL: 'Basketball',
  PICKLEBALL: 'Pickleball',
  TABLE_TENNIS: 'Table Tennis',
};

// Hours available for booking (6am to 10pm)
const BOOKING_HOURS = Array.from({ length: 4 }, (_, i) => i + 18); // [18,19,20,21]

// In-memory session state per user
// Structure: { [senderId]: { step, data } }
const sessions = {};

const bucketName = 'bv-hoa-bucket';

// ─────────────────────────────────────────────
// BOOKING QUEUE (Sunday 10AM PH time)
// ─────────────────────────────────────────────

const QUEUE_TZ = 'Asia/Manila';
const QUEUE_JOIN_WINDOW_MINUTES = 2; // users who message within this window are queued
const QUEUE_TURN_GRACE_MINUTES = 3; // time to send updated booking during their turn
const QUEUE_NOTIFY_MOVE_BY = 5; // notify every time user moves up by 5 places

function getCurrentQueueWindow(now = DateTime.now()) {
  const dt = now.setZone(QUEUE_TZ);

  // Luxon weekday: 1=Mon ... 7=Sun
  const daysSinceSunday = dt.weekday % 7; // Sunday -> 0
  let sunday = dt.minus({ days: daysSinceSunday }).startOf('day');
  let windowStart = sunday.set({ hour: 10, minute: 0, second: 0, millisecond: 0 });

  // If it's before Sunday 10:00 AM, use previous week's window
  if (dt < windowStart) {
    windowStart = windowStart.minus({ days: 7 });
  }

  const windowEnd = windowStart.plus({ minutes: QUEUE_JOIN_WINDOW_MINUTES });
  const windowId = windowStart.toFormat("yyyyLLdd'T'HHmm"); // stable Firestore-safe id

  const isJoinWindow = dt >= windowStart && dt < windowEnd;
  const hasOpened = dt >= windowStart;

  return {
    windowId,
    windowStart,
    windowEnd,
    isJoinWindow,
    hasOpened,
  };
}

function tsFromDateTime(dt) {
  return Firestore.Timestamp.fromDate(dt.toJSDate());
}

function nowTs() {
  return Firestore.Timestamp.now();
}

async function getQueueEntryRef(windowId, senderId) {
  const entryId = `${windowId}_${senderId}`;
  return firestore.collection('queueEntries').doc(entryId);
}

async function getQueuePosition(windowId, entryId) {
  const snapshot = await firestore
    .collection('queueEntries')
    .where('windowId', '==', windowId)
    .orderBy('createdAt')
    .get();

  const activeStatuses = new Set(['queued', 'active', 'waiting_new_booking']);
  const active = snapshot.docs
    .map(d => ({ id: d.id, ...d.data() }))
    .filter(e => activeStatuses.has(e.status));

  const idx = active.findIndex(e => e.id === entryId);
  return idx === -1 ? null : idx + 1;
}

async function ensureQueueEntry(window, senderId, bookingMessage) {
  const entryRef = await getQueueEntryRef(window.windowId, senderId);
  const entryId = entryRef.id;

  await firestore.runTransaction(async tx => {
    const existing = await tx.get(entryRef);
    if (!existing.exists) {
      tx.set(entryRef, {
        windowId: window.windowId,
        senderId,
        createdAt: nowTs(),
        status: 'queued',
        pendingBookingMessage: bookingMessage || null,
        pendingBookingUpdatedAt: bookingMessage ? nowTs() : null,
        lastUserMessageAt: bookingMessage ? nowTs() : null,
        lastProcessedBookingUpdatedAt: null,
        lastNotifiedPosition: null,
        turnStartedAt: null,
        turnExpiresAt: null,
        result: { confirmed: [], conflicts: [], invalid: [] },
      });
      return;
    }

    const data = existing.data() || {};
    // Only allow updates if entry is still relevant
    if (['queued', 'active', 'waiting_new_booking'].includes(data.status)) {
      tx.update(entryRef, {
        pendingBookingMessage: bookingMessage || data.pendingBookingMessage || null,
        pendingBookingUpdatedAt: bookingMessage ? nowTs() : data.pendingBookingUpdatedAt || null,
        lastUserMessageAt: bookingMessage ? nowTs() : data.lastUserMessageAt || null,
      });
    }
  });

  return { entryRef, entryId };
}

async function updateQueueStateLease(tx, stateRef, ownerId, leaseSeconds = 30) {
  const now = DateTime.utc();
  const leaseExpiresAt = tsFromDateTime(now.plus({ seconds: leaseSeconds }));

  const snap = await tx.get(stateRef);
  const state = snap.exists ? snap.data() : {};

  if (state?.leaseExpiresAt && state.leaseExpiresAt.toDate() > new Date()) {
    return { acquired: false, state };
  }

  tx.set(
    stateRef,
    {
      leaseOwner: ownerId,
      leaseExpiresAt,
      updatedAt: nowTs(),
    },
    { merge: true }
  );

  return { acquired: true, state };
}

async function handleBookingMessage(senderId, userMessage) {
  const text = userMessage?.trim();
  const window = getCurrentQueueWindow();

  // Look up existing queue entry for current window (if any)
  const entryRef = await getQueueEntryRef(window.windowId, senderId);
  const entrySnap = await entryRef.get();
  const hasActiveQueueEntry =
    entrySnap.exists && ['queued', 'active', 'waiting_new_booking'].includes(entrySnap.data()?.status);

  if (window.isJoinWindow) {
    const { entryId } = await ensureQueueEntry(window, senderId, text);
    const position = await getQueuePosition(window.windowId, entryId);

    await sendText(
      senderId,
      `✅ You’re in the queue for this week’s booking release.${position ? ` You are #${position}.` : ''}\n\n⚠️ You can send booking details anytime. I’ll process your latest message when it’s your turn.`
    );
    return;
  }

  if (hasActiveQueueEntry) {
    const { entryId } = await ensureQueueEntry(window, senderId, text);
    const position = await getQueuePosition(window.windowId, entryId);

    await sendText(
      senderId,
      `⚠️ You’re currently in the queue${position ? ` (#${position})` : ''}. I saved your latest booking request and will process it when it’s your turn.`
    );
    return;
  }

  // Not in join window and not already queued → process immediately
  await processBookingRequest(senderId, text);
}

// ─────────────────────────────────────────────
// WEBHOOK VERIFICATION
// ─────────────────────────────────────────────

app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('Webhook verified');
    res.status(200).send(challenge);
  } else {
    res.sendStatus(403);
  }
});

// ─────────────────────────────────────────────
// MAIN WEBHOOK RECEIVER
// ─────────────────────────────────────────────

app.post('/webhook', async (req, res) => {
  const body = req.body;
  if (body.object === 'page') {
    for (const entry of body.entry) {
      const event = entry.messaging[0];
      if (!event) continue;
      const senderId = event.sender.id;

      try {
        if (event.postback) {
          await handlePostback(senderId, event.postback.payload);
        } else if (event.message) {
          await handleMessage(senderId, event.message);
        }
      } catch (err) {
        console.error('Handler error:', err);
        await sendText(senderId, '❌ Something went wrong. Please try again.');
      }
    }
    res.status(200).send('EVENT_RECEIVED');
  } else {
    res.sendStatus(404);
  }
});

// ─────────────────────────────────────────────
// POSTBACK HANDLER (Button taps)
// ─────────────────────────────────────────────

async function handlePostback(senderId, payload) {
  switch (payload) {
    case 'MENU_COURT_BOOKINGS':
      await showCourtBookingsMenu(senderId);
      break;
    case 'MENU_SUBMIT_RECEIPT':
      await startReceiptFlow(senderId);
      break;
    case 'MENU_CAT_CLUB':
      await startCatClub(senderId);
      break;
    case 'COURT_VIEW_THIS_WEEK':
      await sendCalendarImage(senderId, 'this');
      break;
    case 'COURT_VIEW_NEXT_WEEK':
      await sendCalendarImage(senderId, 'next');
      break;
    case 'COURT_BOOK':
      await startBookingFlow(senderId);
      break;
    case 'COURT_CANCEL':
      await sendCancelRedirect(senderId);
      break;
    case 'RECEIPT_SKIP_PURPOSE':
      await finalizeReceipt(senderId, null);
      break;
    default:
      await showMainMenu(senderId);
  }
}

// ─────────────────────────────────────────────
// MESSAGE HANDLER (Text + Attachments)
// ─────────────────────────────────────────────

async function handleMessage(senderId, message) {
  const session = sessions[senderId];

  // ── Receipt flow ──
  if (session?.step === 'RECEIPT_AWAIT_UNIT') {
    sessions[senderId].data.unitNumber = message.text?.trim();
    sessions[senderId].step = 'RECEIPT_AWAIT_IMAGE';
    await sendText(senderId, 'Got it! Now please upload a screenshot of your receipt:');
    return;
  }

  if (session?.step === 'RECEIPT_AWAIT_IMAGE') {
    const attachment = message.attachments?.[0];
    if (!attachment || attachment.type !== 'image') {
      await sendText(senderId, '⚠️ Please send an image file of your receipt.');
      return;
    }
    sessions[senderId].data.imageUrl = attachment.payload.url;
    sessions[senderId].step = 'RECEIPT_AWAIT_PURPOSE';
    await sendTextWithButtons(senderId,
      'What is this payment for? (optional)',
      [{ type: 'postback', title: 'Skip', payload: 'RECEIPT_SKIP_PURPOSE' }]
    );
    return;
  }

  if (session?.step === 'RECEIPT_AWAIT_PURPOSE') {
    await finalizeReceipt(senderId, message.text?.trim());
    return;
  }

  // ── Booking flow ──
  if (session?.step === 'BOOKING_AWAIT_DETAILS') {
    await handleBookingMessage(senderId, message.text?.trim());
    return;
  }

  // ── Cat Club flow ──
  if (session?.step === 'CAT_AWAIT_NAME') {
    await lookupCat(senderId, message.text?.trim());
    return;
  }

  // ── Default: show main menu ──
  await showMainMenu(senderId);
}

// ─────────────────────────────────────────────
// MAIN MENU
// ─────────────────────────────────────────────

async function showMainMenu(senderId) {
  sessions[senderId] = null;
  await sendTextWithButtons(senderId,
    'Welcome to BV HOA! 🏘️ What would you like to do today?',
    [
      { type: 'postback', title: '🏀 Court Bookings', payload: 'MENU_COURT_BOOKINGS' },
      { type: 'postback', title: '🧾 Submit Receipt', payload: 'MENU_SUBMIT_RECEIPT' },
      { type: 'postback', title: '🐱 Cat Club', payload: 'MENU_CAT_CLUB' },
    ]
  );
}

// ─────────────────────────────────────────────
// FLOW 1: COURT BOOKINGS
// ─────────────────────────────────────────────

async function showCourtBookingsMenu(senderId) {
  await sendTextWithButtons(senderId,
    'Court Bookings 🏀🏓 — What would you like to do?',
    [
      { type: 'postback', title: "📅 This Week's Bookings", payload: 'COURT_VIEW_THIS_WEEK' },
      { type: 'postback', title: "📅 Next Week's Bookings", payload: 'COURT_VIEW_NEXT_WEEK' },
      { type: 'postback', title: '➕ Book a Court', payload: 'COURT_BOOK' },
      { type: 'postback', title: '❌ Cancel a Booking', payload: 'COURT_CANCEL' },
    ]
  );
}

async function startBookingFlow(senderId) {
  sessions[senderId] = { step: 'BOOKING_AWAIT_DETAILS', data: {} };
  await sendText(senderId,
    'Please describe your booking in one message with the following details:\n\n' +
    '- Facility type (e.g., Basketball, Pickleball, Table Tennis)\n' +
    '- Court (if applicable, e.g., Court A, B, C)\n' +
    '- Date\n' +
    '- Time (start–end)\n' +
    '- Unit / Resident\n' +
    '- Name\n\n' +
    'Example:\n' +
    '"Pickleball Court A, April 16, 7:00–8:00 PM, Unit 716, Juan Dela Cruz"\n\n' +
    'Available courts:\n' +
    '• Basketball — Court 1, 2, Full\n' +
    '• Pickleball — Court A, B, C\n' +
    '• Table Tennis'
  );
}

async function processBookingRequest(senderId, userMessage) {
  await sendText(senderId, 'Let me check that for you...');

  // Parse with OpenAI
  let parsed;
  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-5-mini',
      messages: [
        {
          role: 'system',
          content: `You are a court booking assistant for a village HOA. 
Extract booking details from the user's message and return ONLY a valid JSON object with no markdown or extra text.
The user may request ONE or MULTIPLE bookings in a single message.
Always return this format:
{
  "bookings": [
    {
      "sport": "Basketball" | "Pickleball" | "Table Tennis" | null,
      "court": "A" | "B" | "C" | "1" | "2" | "Table Tennis" | "Full" | null,
      "date": "YYYY-MM-DD" | null,
      "start_hour": <integer 18-21, 24hr format> | null,
      "end_hour": <integer 19-22, 24hr format> | null,
      "name": "<person's name>" | null,
      "unit": "###" | null
    }
  ]
}
If the user only mentions one booking, still return a single-element "bookings" array.
Today's date is ${new Date().toISOString().split('T')[0]}.
For Table Tennis there is only one court — set court to "Table Tennis".
Basketball courts are "1", "2", or "Full".
Pickleball courts are "A", "B", or "C".`
        },
        { role: 'user', content: userMessage }
      ]
    });
    const raw = response.choices[0].message.content.trim();
    parsed = JSON.parse(raw);
  } catch (err) {
    console.error('OpenAI parse error:', err);
    await sendText(senderId, "❌ I couldn't understand that booking. Please try again.\n\nExample: \"Basketball Court A, April 16, 7pm, Juan dela Cruz\"");
    return;
  }

  return await processParsedBookings(senderId, parsed, { sendSummary: true });
}

async function processParsedBookings(senderId, parsed, { sendSummary }) {
  const bookings = Array.isArray(parsed?.bookings)
    ? parsed.bookings
    : parsed
    ? [parsed]
    : [];

  if (!bookings.length) {
    if (sendSummary) {
      await sendText(senderId, `⚠️ I'm missing some details. Please try again and ensure all details are provided and valid.`);
      sessions[senderId] = { step: 'BOOKING_AWAIT_DETAILS', data: {} };
    }
    return {
      confirmed: [],
      conflicts: [],
      invalid: [{ booking: {}, reason: 'missing_details' }],
      summaryText: `⚠️ I'm missing some details. Please try again and ensure all details are provided and valid.`,
    };
  }

  // If any booking is missing a name, try to fetch it from the user's profile
  if (bookings.some(b => !b.name)) {
    const profileName = await getUserProfileName(senderId);
    if (profileName) {
      bookings.forEach(b => {
        if (!b.name) b.name = profileName;
      });
    }
  }

  const confirmed = [];
  const failed = [];

  for (const b of bookings) {
    if (b.end_hour === null || b.end_hour === undefined) {
      b.end_hour = b.start_hour != null ? b.start_hour + 1 : null; // Default to 1 hour booking if end time not provided
    }

    // Validate required fields per booking
    if (!b.sport || !b.court || !b.date || b.start_hour === null || b.start_hour === undefined || !b.unit || !b.name) {
      failed.push({ booking: b, reason: 'missing_details' });
      continue;
    }

    // Validate sport/court compatibility after OpenAI parsing
    const isValidSportCourt =
      (b.sport === 'Basketball' && (b.court === '1' || b.court === '2' || b.court === 'Full')) ||
      (b.sport === 'Pickleball' && (b.court === 'A' || b.court === 'B' || b.court === 'C')) ||
      (b.sport === 'Table Tennis' && b.court === 'Table Tennis');

    if (!isValidSportCourt) {
      failed.push({ booking: b, reason: 'missing_details' });
      continue;
    }

    // Conflict check for each booking
    const hasConflict = await checkConflict(b.sport, b.court, b.date, b.start_hour, b.end_hour);
    if (hasConflict) {
      failed.push({ booking: b, reason: 'conflict' });
      continue;
    }

    // Write non-conflicting booking to Firestore
    await firestore.collection('bookings').add({
      sport: b.sport,
      court: b.court,
      date: b.date,
      start_hour: b.start_hour,
      end_hour: b.end_hour,
      unit: b.unit,
      bookedBy: b.name,
      messengerUserId: senderId,
      createdAt: Firestore.Timestamp.now(),
    });

    confirmed.push(b);
  }

  if (!confirmed.length && !failed.length) {
    if (sendSummary) {
      await sendText(senderId, `⚠️ I'm missing some details. Please try again and ensure all details are provided and valid.`);
      sessions[senderId] = { step: 'BOOKING_AWAIT_DETAILS', data: {} };
    }
    return {
      confirmed: [],
      conflicts: [],
      invalid: [{ booking: {}, reason: 'missing_details' }],
      summaryText: `⚠️ I'm missing some details. Please try again and ensure all details are provided and valid.`,
    };
  }

  const lines = [];

  if (confirmed.length) {
    lines.push('✅ The following bookings are confirmed:');
    confirmed.forEach(b => {
      lines.push(
        `• ${b.sport} — Court ${b.court} on ${b.date}, ${formatHour(b.start_hour)}–${formatHour(
          b.end_hour
        )} (Unit ${b.unit}, ${b.name})`
      );
    });
    lines.push(''); // blank line between sections
  }

  const conflicts = failed.filter(f => f.reason === 'conflict');
  if (conflicts.length) {
    lines.push('❌ These bookings could not be made due to conflicts with existing bookings:');
    conflicts.forEach(({ booking: b }) => {
      lines.push(
        `• ${b.sport} — Court ${b.court} on ${b.date}, ${formatHour(b.start_hour)}–${formatHour(
          b.end_hour
        )} (Unit ${b.unit || 'N/A'}, ${b.name || 'N/A'})`
      );
    });
    lines.push('');
  }

  const missingDetails = failed.filter(f => f.reason === 'missing_details');
  if (missingDetails.length) {
    lines.push(
      `⚠️ I'm missing some details. Please try again and ensure all details are provided and valid.`
    );
    missingDetails.forEach(({ booking: b }) => {
      lines.push(
        `• ${b.sport || 'Sport?'} — Court ${b.court || 'Court?'} on ${b.date || 'Date?'}${
          b.start_hour != null ? `, ${formatHour(b.start_hour)}` : ''
        } (Unit ${b.unit || 'Unit?'}, ${b.name || 'Name?'})`
      );
    });
  }

  const summary = lines.join('\n');

  if (sendSummary) {
    await sendText(senderId, summary);
    sessions[senderId] = confirmed.length ? null : { step: 'BOOKING_AWAIT_DETAILS', data: {} };
  }

  return {
    confirmed,
    conflicts: failed.filter(f => f.reason === 'conflict').map(f => f.booking),
    invalid: failed.filter(f => f.reason === 'missing_details').map(f => f.booking),
    summaryText: summary,
  };
}

async function processBookingRequestSilently(senderId, userMessage) {
  // Used by queue dispatcher when it is the user's turn.
  // Returns structured results and does not touch session state.
  let parsed;
  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-5-mini',
      messages: [
        {
          role: 'system',
          content: `You are a court booking assistant for a village HOA. 
Extract booking details from the user's message and return ONLY a valid JSON object with no markdown or extra text.
The user may request ONE or MULTIPLE bookings in a single message.
Always return this format:
{
  "bookings": [
    {
      "sport": "Basketball" | "Pickleball" | "Table Tennis" | null,
      "court": "A" | "B" | "C" | "1" | "2" | "Table Tennis" | "Full" | null,
      "date": "YYYY-MM-DD" | null,
      "start_hour": <integer 18-21, 24hr format> | null,
      "end_hour": <integer 19-22, 24hr format> | null,
      "name": "<person's name>" | null,
      "unit": "###" | null
    }
  ]
}
If the user only mentions one booking, still return a single-element "bookings" array.
Today's date is ${new Date().toISOString().split('T')[0]}.
For Table Tennis there is only one court — set court to "Table Tennis".
Basketball courts are "1", "2", or "Full".
Pickleball courts are "A", "B", or "C".`
        },
        { role: 'user', content: userMessage }
      ]
    });
    const raw = response.choices[0].message.content.trim();
    parsed = JSON.parse(raw);
  } catch (err) {
    console.error('OpenAI parse error (silent):', err);
    return {
      confirmed: [],
      conflicts: [],
      invalid: [{ booking: {}, reason: 'parse_error' }],
      summaryText:
        `❌ I couldn't understand that booking. Please try again.\n\nExample: "Basketball Court 1, April 16, 7pm, Unit 716, Juan dela Cruz"`,
    };
  }

  return await processParsedBookings(senderId, parsed, { sendSummary: false });
}

async function checkConflict(sport, court, date, start_hour, end_hour) {
  const newStart = start_hour * 60;
  const newEnd = end_hour * 60;

  const snapshot = await firestore.collection('bookings')
    .where('date', '==', date)
    .get();

  return snapshot.docs.some(doc => {
    const b = doc.data();

    const existingStart = b.start_hour * 60;
    const existingEnd = b.end_hour * 60;

    const timeOverlap =
      existingStart < newEnd && existingEnd > newStart;

    if (!timeOverlap) return false;

    // -----------------------------
    // 1. Table Tennis is isolated
    // -----------------------------
    if (court === 'Table Tennis' || b.court === 'Table Tennis') {
      return court === 'Table Tennis' && b.court === 'Table Tennis';
    }

    // -----------------------------
    // 2. FULL court rules
    // -----------------------------
    const newIsFull = court === 'Full';
    const existingIsFull = b.court === 'Full';

    if (newIsFull || existingIsFull) {
      // FULL blocks ANY A/B/C and vice versa
      return true;
    }

    // -----------------------------
    // 3. Normal courts (A/B/C)
    // -----------------------------
    return b.court === court;
  });
}

app.post('/tasks/dispatch-queue', async (req, res) => {
  try {
    const token = req.headers['x-queue-token'];
    if (!QUEUE_TASK_TOKEN || token !== QUEUE_TASK_TOKEN) {
      return res.status(403).send('Forbidden');
    }

    await dispatchBookingQueue();
    return res.status(200).send('OK');
  } catch (err) {
    console.error('dispatch-queue error:', err);
    return res.status(500).send('Error');
  }
});

async function dispatchBookingQueue() {
  // Guard: only run dispatcher on Sundays 9:50–10:30 AM Philippine time.
  // This keeps Cloud Run invocations cheap outside the queue rush window.
  const dt = DateTime.now().setZone(QUEUE_TZ);
  const isSunday = dt.weekday === 7; // Luxon: 7 = Sunday
  const minutesSinceMidnight = dt.hour * 60 + dt.minute;
  const start = 9 * 60 + 50; // 09:50
  const end = 10 * 60 + 30; // 10:30
  const inRushWindow = minutesSinceMidnight >= start && minutesSinceMidnight <= end;
  if (!isSunday || !inRushWindow) return;

  const window = getCurrentQueueWindow();
  const windowId = window.windowId;

  // Acquire lease on queue state so only one dispatcher runs
  const stateRef = firestore.collection('queueState').doc('bookingQueue');
  const ownerId = `dispatcher_${process.pid}`;

  const leaseResult = await firestore.runTransaction(async tx => {
    const { acquired, state } = await updateQueueStateLease(tx, stateRef, ownerId, 30);
    if (!acquired) return { acquired: false, state };

    // Ensure state doc has window info
    tx.set(
      stateRef,
      {
        windowId,
        windowStartAt: tsFromDateTime(window.windowStart),
        windowEndAt: tsFromDateTime(window.windowEnd),
        isOpen: window.isJoinWindow,
        updatedAt: nowTs(),
      },
      { merge: true }
    );
    return { acquired: true, state };
  });

  if (!leaseResult.acquired) return;

  // Notify position improvements (every +5)
  await notifyQueueMovements(windowId);

  const stateSnap = await stateRef.get();
  const state = stateSnap.exists ? stateSnap.data() : {};
  const activeEntryId = state?.activeEntryId || null;

  if (activeEntryId) {
    await handleActiveTurn(windowId, stateRef, activeEntryId);
    return;
  }

  // Activate next queued entry
  const nextSnap = await firestore
    .collection('queueEntries')
    .where('windowId', '==', windowId)
    .where('status', '==', 'queued')
    .orderBy('createdAt')
    .limit(1)
    .get();

  if (nextSnap.empty) return;

  const nextDoc = nextSnap.docs[0];
  const nextEntryId = nextDoc.id;

  await firestore.runTransaction(async tx => {
    const entry = await tx.get(nextDoc.ref);
    if (!entry.exists) return;
    const data = entry.data();
    if (data.status !== 'queued') return;

    const now = DateTime.utc();
    tx.update(nextDoc.ref, {
      status: 'active',
      turnStartedAt: tsFromDateTime(now),
      turnExpiresAt: tsFromDateTime(now.plus({ minutes: QUEUE_TURN_GRACE_MINUTES })),
    });
    tx.set(stateRef, { activeEntryId: nextEntryId, updatedAt: nowTs() }, { merge: true });
  });

  const activatedSnap = await nextDoc.ref.get();
  if (!activatedSnap.exists) return;
  const activated = activatedSnap.data();

  await sendText(activated.senderId, `✅ It's your turn! Here's the current week's calendar.`);
  await sendCalendarImage(activated.senderId, 'this');

  // If user already sent a booking message while queued, process it now
  if (activated.pendingBookingMessage) {
    await handleActiveTurn(windowId, stateRef, nextEntryId);
  } else {
    await sendText(
      activated.senderId,
      `⚠️ Please send your booking details now. If you don't send an updated booking within ${QUEUE_TURN_GRACE_MINUTES} minutes, your turn will be forfeited.`
    );
  }
}

async function notifyQueueMovements(windowId) {
  const snapshot = await firestore
    .collection('queueEntries')
    .where('windowId', '==', windowId)
    .orderBy('createdAt')
    .get();

  const activeStatuses = new Set(['queued', 'active', 'waiting_new_booking']);
  const entries = snapshot.docs.map(d => ({ id: d.id, ref: d.ref, ...d.data() })).filter(e => activeStatuses.has(e.status));

  // Positions are by createdAt among active statuses
  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    if (e.status !== 'queued') continue; // only notify queued users

    const position = i + 1;
    const last = e.lastNotifiedPosition;

    if (last == null) {
      await e.ref.update({ lastNotifiedPosition: position });
      continue;
    }

    if (last - position >= QUEUE_NOTIFY_MOVE_BY) {
      await sendText(e.senderId, `✅ You moved up in the queue — you're now #${position}.`);
      await e.ref.update({ lastNotifiedPosition: position });
    }
  }
}

async function handleActiveTurn(windowId, stateRef, entryId) {
  const entryRef = firestore.collection('queueEntries').doc(entryId);
  const entrySnap = await entryRef.get();
  if (!entrySnap.exists) {
    await stateRef.set({ activeEntryId: null, updatedAt: nowTs() }, { merge: true });
    return;
  }

  const entry = entrySnap.data();
  if (entry.windowId !== windowId) {
    // Active entry belongs to previous window; clear it
    await stateRef.set({ activeEntryId: null, updatedAt: nowTs() }, { merge: true });
    return;
  }

  // Forfeit if expired and still waiting for new booking
  const expires = entry.turnExpiresAt?.toDate?.() || null;
  if (expires && new Date() > expires && (entry.status === 'active' || entry.status === 'waiting_new_booking')) {
    // If they already sent something and it was processed, don't forfeit here; otherwise forfeit
    const hasPending =
      entry.pendingBookingUpdatedAt &&
      (!entry.lastProcessedBookingUpdatedAt ||
        entry.pendingBookingUpdatedAt.toMillis() > entry.lastProcessedBookingUpdatedAt.toMillis());

    if (!hasPending) {
      await entryRef.update({ status: 'forfeited' });
      await sendText(entry.senderId, '❌ Your turn expired (no updated booking received in time). Please re-queue if you still want to book.');
      await stateRef.set({ activeEntryId: null, updatedAt: nowTs() }, { merge: true });
      return;
    }
  }

  // Process latest pending booking message if it has not been processed yet
  const pendingUpdatedAt = entry.pendingBookingUpdatedAt || null;
  const lastProcessedAt = entry.lastProcessedBookingUpdatedAt || null;
  const shouldProcess =
    entry.pendingBookingMessage &&
    pendingUpdatedAt &&
    (!lastProcessedAt || pendingUpdatedAt.toMillis() > lastProcessedAt.toMillis());

  if (!shouldProcess) return;

  const result = await processBookingRequestSilently(entry.senderId, entry.pendingBookingMessage);

  // Always send booking summary for turn processing
  if (result?.summaryText) {
    await sendText(entry.senderId, result.summaryText);
  }

  const allAccepted = result.invalid.length === 0 && result.conflicts.length === 0 && result.confirmed.length > 0;
  const anyRejected = result.invalid.length > 0 || result.conflicts.length > 0;

  if (anyRejected) {
    const now = DateTime.utc();
    await entryRef.update({
      status: 'waiting_new_booking',
      lastProcessedBookingUpdatedAt: pendingUpdatedAt,
      turnExpiresAt: tsFromDateTime(now.plus({ minutes: QUEUE_TURN_GRACE_MINUTES })),
      result: {
        confirmed: result.confirmed,
        conflicts: result.conflicts,
        invalid: result.invalid,
      },
    });

    await sendText(
      entry.senderId,
      `⚠️ Some bookings could not be accepted. Please send an updated booking message within ${QUEUE_TURN_GRACE_MINUTES} minutes or your turn will be forfeited.`
    );
    return;
  }

  if (allAccepted) {
    await entryRef.update({
      status: 'completed',
      lastProcessedBookingUpdatedAt: pendingUpdatedAt,
      result: {
        confirmed: result.confirmed,
        conflicts: result.conflicts,
        invalid: result.invalid,
      },
    });
    await stateRef.set({ activeEntryId: null, updatedAt: nowTs() }, { merge: true });
  }
}

async function sendCancelRedirect(senderId) {
  await sendTextWithButtons(senderId,
    'To cancel a booking, please message our admin directly. They will assist you shortly.',
    [{ type: 'url', title: '💬 Message Admin', url: `https://m.me/${ADMIN_MESSENGER_ID}` }]
  );
}

// ─────────────────────────────────────────────
// CALENDAR IMAGE GENERATOR
// ─────────────────────────────────────────────

async function sendCalendarImage(senderId, week) {
  await sendText(senderId, `Generating ${week === 'this' ? "this" : "next"} week's calendar...`);

  // -----------------------------
  // Build week range (Mon–Sun)
  // -----------------------------
  const today = new Date();
  const dayOfWeek = today.getDay();

  const monday = new Date(today);
  monday.setDate(today.getDate() - (dayOfWeek === 0 ? 6 : dayOfWeek - 1));

  if (week === 'next') monday.setDate(monday.getDate() + 7);

  const days = Array.from({ length: 7 }, (_, i) => {
    const d = new Date(monday);
    d.setDate(monday.getDate() + i);
    return d;
  });

  const dateStrings = days.map(d => d.toISOString().split('T')[0]);

  const startDate = dateStrings[0];
  const endDate = dateStrings[6];

  // -----------------------------
  // Fetch bookings
  // -----------------------------
  const snapshot = await firestore.collection('bookings')
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .get();

  // -----------------------------
  // Normalize bookings
  // -----------------------------
  const bookingsByDate = {};

  snapshot.forEach(doc => {
    const b = doc.data();

    if (!bookingsByDate[b.date]) bookingsByDate[b.date] = [];

    bookingsByDate[b.date].push({
      sport: b.sport,
      court: b.court,
      start: b.start_hour * 60,
      end: b.end_hour * 60,
      bookedBy: b.bookedBy
    });
  });

  function overlaps(b, slotStart, slotEnd) {
    return b.start < slotEnd && b.end > slotStart;
  }

  function getCourtLabel(b) {
    if (b.court === 'Table Tennis') return 'TT';
    if (b.court === 'FULL') return 'BB FULL';

    return b.sport === SPORTS.BASKETBALL
      ? `BB ${b.court}`
      : b.sport === SPORTS.PICKLEBALL
      ? `PB ${b.court}`
      : `${b.sport.substring(0, 2).toUpperCase()} ${b.court}`;
  }

  // -----------------------------
  // Canvas setup
  // -----------------------------
  const COL_WIDTH = 110;
  const ROW_HEIGHT = 52;
  const HEADER_HEIGHT = 60;
  const TIME_COL_WIDTH = 64;

  const WIDTH = TIME_COL_WIDTH + COL_WIDTH * 7;
  const HEIGHT = HEADER_HEIGHT + ROW_HEIGHT * BOOKING_HOURS.length;

  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext('2d');

  // Background
  ctx.fillStyle = '#F8F9FA';
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  // Header
  ctx.fillStyle = '#1A237E';
  ctx.fillRect(0, 0, WIDTH, HEADER_HEIGHT);

  ctx.fillStyle = '#FFFFFF';
  ctx.font = 'bold 15px sans-serif';
  ctx.textAlign = 'center';

  const weekLabel =
    `${week === 'this' ? 'This' : 'Next'} Week: ${formatDate(days[0])} – ${formatDate(days[6])}`;

  ctx.fillText(weekLabel, WIDTH / 2, 22);

  // -----------------------------
  // Day headers
  // -----------------------------
  const DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

  ctx.font = 'bold 12px sans-serif';

  days.forEach((d, i) => {
    const x = TIME_COL_WIDTH + COL_WIDTH * i + COL_WIDTH / 2;

    ctx.fillStyle = '#FFFFFF';
    ctx.fillText(DAY_NAMES[i], x, 42);

    ctx.font = '11px sans-serif';
    ctx.fillStyle = '#90CAF9';
    ctx.fillText(formatDate(d, true), x, 56);

    ctx.font = 'bold 12px sans-serif';
  });

  // -----------------------------
  // GRID RENDERING
  // -----------------------------
  BOOKING_HOURS.forEach((hour, rowIdx) => {
    const y = HEADER_HEIGHT + ROW_HEIGHT * rowIdx;
    const isEven = rowIdx % 2 === 0;

    ctx.fillStyle = isEven ? '#FFFFFF' : '#F0F4FF';
    ctx.fillRect(TIME_COL_WIDTH, y, WIDTH - TIME_COL_WIDTH, ROW_HEIGHT);

    // Time label
    ctx.fillStyle = '#37474F';
    ctx.font = 'bold 11px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(formatHour(hour), TIME_COL_WIDTH - 6, y + ROW_HEIGHT / 2 + 4);

    const slotStart = hour * 60;
    const slotEnd = (hour + 1) * 60;

    // -----------------------------
    // Render each day column
    // -----------------------------
    days.forEach((d, colIdx) => {
      const x = TIME_COL_WIDTH + COL_WIDTH * colIdx;
      const dateStr = dateStrings[colIdx];

      const dayBookings = bookingsByDate[dateStr] || [];

      // Cell border
      ctx.strokeStyle = '#CFD8DC';
      ctx.lineWidth = 0.5;
      ctx.strokeRect(x, y, COL_WIDTH, ROW_HEIGHT);

      dayBookings.forEach(b => {
        if (!overlaps(b, slotStart, slotEnd)) return;

        // Visible portion of booking inside this cell
        const visibleStart = Math.max(b.start, slotStart);
        const visibleEnd = Math.min(b.end, slotEnd);

        const ratioStart = (visibleStart - slotStart) / 60;
        const ratioEnd = (visibleEnd - slotStart) / 60;

        const blockY = y + ratioStart * ROW_HEIGHT;
        const blockHeight = Math.max(2, (ratioEnd - ratioStart) * ROW_HEIGHT);

        // FULL court spans 3 columns
        let blockWidth = COL_WIDTH;
        if (b.court === 'FULL') {
          blockWidth = COL_WIDTH * 3;
        }

        const label = `${getCourtLabel(b)}\n${b.bookedBy.split(' ')[0]}`;

        ctx.fillStyle = getChipColor(label);
        roundRect(ctx, x + 3, blockY + 2, blockWidth - 6, blockHeight - 4, 4);

        ctx.fillStyle = '#FFFFFF';
        ctx.font = `bold ${blockHeight > 18 ? 9 : 8}px sans-serif`;
        ctx.textAlign = 'center';

        label.split('\n').forEach((line, li) => {
          ctx.fillText(
            line,
            x + blockWidth / 2,
            blockY + 12 + li * 10,
            blockWidth - 10
          );
        });
      });
    });
  });

  // -----------------------------
  // Grid lines
  // -----------------------------
  ctx.strokeStyle = '#B0BEC5';
  ctx.lineWidth = 1;

  BOOKING_HOURS.forEach((_, rowIdx) => {
    const y = HEADER_HEIGHT + ROW_HEIGHT * rowIdx;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(WIDTH, y);
    ctx.stroke();
  });

  ctx.strokeStyle = '#90A4AE';
  ctx.lineWidth = 1.5;

  ctx.beginPath();
  ctx.moveTo(TIME_COL_WIDTH, 0);
  ctx.lineTo(TIME_COL_WIDTH, HEIGHT);
  ctx.stroke();

  // Legend
  ctx.fillStyle = '#37474F';
  ctx.font = '10px sans-serif';
  ctx.textAlign = 'left';
  ctx.fillText(
    'BB=Basketball  PB=Pickleball  TT=Table Tennis  FULL=All Courts A/B/C',
    6,
    HEIGHT - 4
  );

  // -----------------------------
  // Export
  // -----------------------------
  const tmpPath = path.join(os.tmpdir(), `calendar_${Date.now()}.png`);
  const buffer = canvas.toBuffer('image/png');

  fs.writeFileSync(tmpPath, buffer);

  const imageUrl = await uploadImageToMeta(tmpPath);
  fs.unlinkSync(tmpPath);

  await sendImage(senderId, imageUrl);
}

function getChipColor(label) {
  if (label.startsWith('BB')) return '#1565C0'; // Basketball blue
  if (label.startsWith('PI') || label.startsWith('PB')) return '#2E7D32'; // Pickleball green
  if (label.startsWith('TT')) return '#6A1B9A'; // Table Tennis purple
  return '#455A64';
}

function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.quadraticCurveTo(x + w, y, x + w, y + r);
  ctx.lineTo(x + w, y + h - r);
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
  ctx.lineTo(x + r, y + h);
  ctx.quadraticCurveTo(x, y + h, x, y + h - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
  ctx.fill();
}

async function uploadImageToMeta(filePath) {
  const bucket = storage.bucket(bucketName);
  const destination = `temp/calendar_${Date.now()}.png`;

  // 1. Just upload (do not call makePublic)
  await bucket.upload(filePath, {
    destination: destination,
    metadata: {
      contentType: 'image/png',
      cacheControl: 'public, max-age=3600',
    },
  });

  // 2. Return the direct public URL
  return `https://storage.googleapis.com/${bucketName}/${destination}`;
}

// ─────────────────────────────────────────────
// FLOW 2: SUBMIT RECEIPT
// ─────────────────────────────────────────────

async function startReceiptFlow(senderId) {
  sessions[senderId] = { step: 'RECEIPT_AWAIT_UNIT', data: {} };
  await sendText(senderId, '🧾 Receipt Submission\n\nPlease provide your unit number(s):\n\nExample: "Unit 12B" or "Units 5A and 5B"');
}

async function finalizeReceipt(senderId, purpose) {
  const session = sessions[senderId];
  const { unitNumber, imageUrl } = session.data;

  await sendText(senderId, 'Got it! Processing your receipt... 🔍');

  // Download image and convert to base64 for OCR
  let base64Image;
  try {
    const response = await axios.get(imageUrl, { responseType: 'arraybuffer' });
    base64Image = Buffer.from(response.data).toString('base64');
  } catch (err) {
    console.error('Image download error:', err);
    await sendText(senderId, '❌ Failed to download your image. Please try again.');
    return;
  }

  // OCR via OpenAI Vision
  let ocrData = { referenceNumber: null, date: null, amount: null };
  try {
    const ocrResponse = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'user',
          content: [
            {
              type: 'text',
              text: `Extract the following from this receipt/payment screenshot and return ONLY a JSON object with no markdown:
{
  "referenceNumber": "<reference or transaction number, or null>",
  "date": "<transaction date in YYYY-MM-DD format, or null>",
  "amount": "<transaction amount as string with currency symbol, or null>"
}`
            },
            {
              type: 'image_url',
              image_url: { url: `data:image/jpeg;base64,${base64Image}` }
            }
          ]
        }
      ]
    });
    const raw = ocrResponse.choices[0].message.content.trim();
    ocrData = JSON.parse(raw);
  } catch (err) {
    console.error('OCR error:', err);
    // Non-fatal — continue without OCR data
  }

  // Upload image to Google Drive
  let driveFileId = null;
  try {
    const drive = google.drive({ version: 'v3', auth: googleAuth });
    const tmpPath = path.join(os.tmpdir(), `receipt_${Date.now()}.jpg`);
    const imgResponse = await axios.get(imageUrl, { responseType: 'arraybuffer' });
    fs.writeFileSync(tmpPath, imgResponse.data);

    const month = new Date().toISOString().slice(0, 7); // "2026-04"
    const fileName = `${month}_${unitNumber}_${ocrData.referenceNumber || Date.now()}.jpg`;
    const fileMetadata = { name: fileName, parents: [GOOGLE_DRIVE_FOLDER_ID] };
    const media = { mimeType: 'image/jpeg', body: fs.createReadStream(tmpPath) };
    const file = await drive.files.create({ resource: fileMetadata, media, fields: 'id' });
    driveFileId = file.data.id;
    fs.unlinkSync(tmpPath);
  } catch (err) {
    console.error('Drive upload error:', err);
  }

  // Save metadata to Firestore
  await firestore.collection('receipts').add({
    unitNumber,
    purpose: purpose || null,
    driveFileId,
    referenceNumber: ocrData.referenceNumber,
    date: ocrData.date,
    amount: ocrData.amount,
    messengerUserId: senderId,
    createdAt: Firestore.Timestamp.now(),
  });

  const summary = [
    `✅ Receipt submitted successfully!`,
    ``,
    `📋 Unit: ${unitNumber}`,
    ocrData.referenceNumber ? `🔖 Reference: ${ocrData.referenceNumber}` : null,
    ocrData.amount ? `💰 Amount: ${ocrData.amount}` : null,
    ocrData.date ? `📅 Date: ${ocrData.date}` : null,
    purpose ? `📝 Purpose: ${purpose}` : null,
  ].filter(Boolean).join('\n');

  await sendText(senderId, summary);
  sessions[senderId] = null;
}

// ─────────────────────────────────────────────
// FLOW 3: CAT CLUB
// ─────────────────────────────────────────────

async function startCatClub(senderId) {
  sessions[senderId] = { step: 'CAT_AWAIT_NAME', data: {} };
  await sendText(senderId, '🐱 Welcome to Cat Club!\n\nEnter the name of a cat you\'d like to see:');
}

async function lookupCat(senderId, name) {
  const snapshot = await firestore.collection('cats')
    .where('nameLower', '==', name.toLowerCase().trim())
    .limit(1)
    .get();

  if (snapshot.empty) {
    await sendTextWithButtons(senderId,
      `😿 No cat named "${name}" was found in our records.\n\nMaybe they're hiding! Try another name?`,
      [{ type: 'postback', title: '🔍 Try Another', payload: 'MENU_CAT_CLUB' }]
    );
  } else {
    const cat = snapshot.docs[0].data();
    await sendImage(senderId, cat.imageUrl);
    await sendTextWithButtons(senderId,
      `🐾 Here's ${cat.name}!`,
      [{ type: 'postback', title: '🔍 Search Another Cat', payload: 'MENU_CAT_CLUB' }]
    );
  }
  sessions[senderId] = null;
}

// ─────────────────────────────────────────────
// MESSENGER SEND HELPERS
// ─────────────────────────────────────────────

async function sendText(recipientId, text) {
  await callSendAPI(recipientId, { text });
}

async function sendImage(recipientId, imageUrl) {
  await callSendAPI(recipientId, {
    attachment: {
      type: 'image',
      payload: { url: imageUrl, is_reusable: true },
    },
  });
}

async function sendTextWithButtons(recipientId, text, buttons) {
  // Meta only allows max 3 buttons per template
  const chunks = [];
  for (let i = 0; i < buttons.length; i += 3) chunks.push(buttons.slice(i, i + 3));

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const isFirst = i === 0;
    await callSendAPI(recipientId, {
      attachment: {
        type: 'template',
        payload: {
          template_type: 'button',
          text: isFirst ? text : '—',
          buttons: chunk.map(b => {
            if (b.type === 'postback') {
              return { type: 'postback', title: b.title, payload: b.payload };
            } else if (b.type === 'url') {
              return { type: 'web_url', title: b.title, url: b.url };
            }
            return b;
          }),
        },
      },
    });
  }
}

async function getUserProfileName(senderId) {
  try {
    const response = await axios.get(`https://graph.facebook.com/v19.0/${senderId}`, {
      params: {
        access_token: PAGE_ACCESS_TOKEN,
        fields: 'name,first_name,last_name',
      },
    });
    const data = response.data || {};
    const fullName =
      data.name ||
      [data.first_name, data.last_name].filter(Boolean).join(' ').trim() ||
      null;
    return fullName || null;
  } catch (err) {
    console.error('Failed to fetch user profile name:', err?.response?.data || err.message || err);
    return null;
  }
}

async function callSendAPI(recipientId, message) {
  await axios.post(
    'https://graph.facebook.com/v19.0/me/messages',
    { recipient: { id: recipientId }, message },
    { params: { access_token: PAGE_ACCESS_TOKEN } }
  );
}

// ─────────────────────────────────────────────
// UTILITIES
// ─────────────────────────────────────────────

function formatHour(hour) {
  if (hour === 12) return '12:00 PM';
  if (hour === 0 || hour === 24) return '12:00 AM';
  return hour < 12 ? `${hour}:00 AM` : `${hour - 12}:00 PM`;
}

function formatDate(d, short = false) {
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return short
    ? `${months[d.getMonth()]} ${d.getDate()}`
    : `${months[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
}

// ─────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────

app.get('/', (req, res) => res.send('BV HOA Bot is running ✅'));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
