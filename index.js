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
        await sendText(senderId, 'Something went wrong. Please try again.');
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
      await sendText(senderId, 'Please send an image file of your receipt.');
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
    await processBookingRequest(senderId, message.text?.trim());
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
    'Please describe your booking in one message.\n\nExample:\n"Pickleball Court A, April 16, 7pm to 8pm, Unit 716, Juan dela Cruz"\n\nAvailable courts:\n• Basketball — Court A, B, C\n• Pickleball — Court A, B, C\n• Table Tennis'
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
Format:
{
  "sport": "Basketball" | "Pickleball" | "Table Tennis" | null,
  "court": "A" | "B" | "C" | "Table Tennis" | null,
  "date": "YYYY-MM-DD" | null,
  "hour": <integer 18-21, 24hr format> | null,
  "name": "<person's name>" | null,
  "unit": "###" | null
}
Today's date is ${new Date().toISOString().split('T')[0]}.
For Table Tennis there is only one court — set court to "Table Tennis".
Basketball and Pickleball use courts A, B, or C.`
        },
        { role: 'user', content: userMessage }
      ]
    });
    const raw = response.choices[0].message.content.trim();
    parsed = JSON.parse(raw);
  } catch (err) {
    console.error('OpenAI parse error:', err);
    await sendText(senderId, "I couldn't understand that booking. Please try again.\n\nExample: \"Basketball Court A, April 16, 7pm, Juan dela Cruz\"");
    return;
  }

  // Validate required fields
  if (!parsed.sport || !parsed.court || !parsed.date || parsed.hour === null || !parsed.name || !parsed.unit) {
    await sendText(senderId,
      `I'm missing some details. Please include:\n• Sport (Basketball, Pickleball, or Table Tennis)\n• Court (A, B, or C — not needed for Table Tennis)\n• Date\n• Time (e.g. 7pm)\n• Your name\n• Your unit number`
    );
    return;
  }

  // Conflict check
  const hasConflict = await checkConflict(parsed.sport, parsed.court, parsed.date, parsed.hour);
  if (hasConflict) {
    const timeStr = formatHour(parsed.hour);
    await sendText(senderId,
      `❌ Sorry! ${parsed.sport} Court ${parsed.court} is already booked on ${parsed.date} at ${timeStr}.\n\nPlease choose a different time or court.`
    );
    sessions[senderId] = { step: 'BOOKING_AWAIT_DETAILS', data: {} };
    return;
  }

  // Write to Firestore
  await firestore.collection('bookings').add({
    sport: parsed.sport,
    court: parsed.court,
    date: parsed.date,
    hour: parsed.hour,
    unit: parsed.unit,
    bookedBy: parsed.name,
    messengerUserId: senderId,
    createdAt: Firestore.Timestamp.now(),
  });

  const timeStr = formatHour(parsed.hour);
  await sendText(senderId,
    `✅ Booking confirmed!\n\n🏟️ ${parsed.sport} — Court ${parsed.court}\n📅 ${parsed.date}\n🕐 ${timeStr}–${formatHour(parsed.hour + 1)}\n👤 ${parsed.name}\n\nEnjoy your game! 🎉`
  );
  sessions[senderId] = null;
}

async function checkConflict(sport, court, date, hour) {
  // For shared courts (A, B, C): Basketball and Pickleball share the same physical court
  // So we block by court + date + hour regardless of sport
  let query;

  if (court === 'Table Tennis') {
    // Table Tennis has its own court — only conflicts with other Table Tennis bookings
    query = firestore.collection('bookings')
      .where('court', '==', 'Table Tennis')
      .where('date', '==', date)
      .where('hour', '==', hour);
  } else {
    // Shared courts — conflict if ANY sport booked same court/date/hour
    query = firestore.collection('bookings')
      .where('court', '==', court)
      .where('date', '==', date)
      .where('hour', '==', hour);
  }

  const snapshot = await query.get();
  return !snapshot.empty;
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

  // Calculate Mon–Sun of target week
  const today = new Date();
  const dayOfWeek = today.getDay(); // 0=Sun, 1=Mon...
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

  // Fetch bookings for the week
  const snapshot = await firestore.collection('bookings')
    .where('date', '>=', startDate)
    .where('date', '<=', endDate)
    .get();

  // Organise: bookingMap[date][hour] = [label, ...]
  const bookingMap = {};
  snapshot.forEach(doc => {
    const b = doc.data();
    if (!bookingMap[b.date]) bookingMap[b.date] = {};
    if (!bookingMap[b.date][b.hour]) bookingMap[b.date][b.hour] = [];
    const courtLabel = b.court === 'Table Tennis' ? 'TT' : `${b.sport.substring(0, 2).toUpperCase()} ${b.court}`
    b.sport === SPORTS.BASKETBALL ? courtLabel = `BB ${b.court}` :
    b.sport === SPORTS.PICKLEBALL ? courtLabel = `PB ${b.court}` : 'UNK';
    bookingMap[b.date][b.hour].push(`${courtLabel}\n${b.bookedBy.split(' ')[0]}`);
  });

  // Canvas dimensions
  const COL_WIDTH = 110;
  const ROW_HEIGHT = 52;
  const HEADER_HEIGHT = 60;
  const TIME_COL_WIDTH = 64;
  const NUM_ROWS = BOOKING_HOURS.length;
  const WIDTH = TIME_COL_WIDTH + COL_WIDTH * 7;
  const HEIGHT = HEADER_HEIGHT + ROW_HEIGHT * NUM_ROWS;

  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext('2d');

  // Background
  ctx.fillStyle = '#F8F9FA';
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  // Header background
  ctx.fillStyle = '#1A237E';
  ctx.fillRect(0, 0, WIDTH, HEADER_HEIGHT);

  // Header title
  ctx.fillStyle = '#FFFFFF';
  ctx.font = 'bold 15px sans-serif';
  ctx.textAlign = 'center';
  const weekLabel = `${week === 'this' ? 'This' : 'Next'} Week: ${formatDate(days[0])} – ${formatDate(days[6])}`;
  ctx.fillText(weekLabel, WIDTH / 2, 22);

  // Day column headers
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

  // Grid rows
  BOOKING_HOURS.forEach((hour, rowIdx) => {
    const y = HEADER_HEIGHT + ROW_HEIGHT * rowIdx;
    const isEven = rowIdx % 2 === 0;

    // Row background
    ctx.fillStyle = isEven ? '#FFFFFF' : '#F0F4FF';
    ctx.fillRect(TIME_COL_WIDTH, y, WIDTH - TIME_COL_WIDTH, ROW_HEIGHT);

    // Time label
    ctx.fillStyle = '#37474F';
    ctx.font = 'bold 11px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(formatHour(hour), TIME_COL_WIDTH - 6, y + ROW_HEIGHT / 2 + 4);

    // Cells
    days.forEach((d, colIdx) => {
      const x = TIME_COL_WIDTH + COL_WIDTH * colIdx;
      const dateStr = dateStrings[colIdx];
      const cellBookings = bookingMap[dateStr]?.[hour] || [];

      // Cell border
      ctx.strokeStyle = '#CFD8DC';
      ctx.lineWidth = 0.5;
      ctx.strokeRect(x, y, COL_WIDTH, ROW_HEIGHT);

      // Booking chips
      if (cellBookings.length > 0) {
        const chipHeight = Math.min(ROW_HEIGHT - 6, (ROW_HEIGHT - 6) / cellBookings.length);
        cellBookings.forEach((label, ci) => {
          const chipY = y + 3 + chipHeight * ci;
          ctx.fillStyle = getChipColor(label);
          roundRect(ctx, x + 3, chipY, COL_WIDTH - 6, chipHeight - 2, 4);
          ctx.fillStyle = '#FFFFFF';
          ctx.font = `bold ${chipHeight > 20 ? 9 : 8}px sans-serif`;
          ctx.textAlign = 'center';
          const lines = label.split('\n');
          lines.forEach((line, li) => {
            ctx.fillText(line, x + COL_WIDTH / 2, chipY + 10 + li * 10, COL_WIDTH - 10);
          });
        });
      }
    });
  });

  // Row dividers
  ctx.strokeStyle = '#B0BEC5';
  ctx.lineWidth = 1;
  BOOKING_HOURS.forEach((_, rowIdx) => {
    const y = HEADER_HEIGHT + ROW_HEIGHT * rowIdx;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(WIDTH, y);
    ctx.stroke();
  });

  // Time column right border
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
  ctx.fillText('BB=Basketball  PB=Pickleball  TT=Table Tennis', 6, HEIGHT - 4);

  // Save and upload
  const tmpPath = path.join(os.tmpdir(), `calendar_${Date.now()}.png`);
  const buffer = canvas.toBuffer('image/png');
  fs.writeFileSync(tmpPath, buffer);

  // Upload to temporary hosting via Imgur-style or just send as attachment
  // Here we use a publicly accessible URL trick via Meta's attachment upload API
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
    await sendText(senderId, 'Failed to download your image. Please try again.');
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
