require('dotenv').config();

const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const { google } = require('googleapis');
const OpenAI = require('openai');

const app = express();
app.use(bodyParser.json({ limit: '1mb' }));

/* =========================
   ENV
========================= */
const PAGE_ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;

/* =========================
   AXIOS (with timeout)
========================= */
const axiosClient = axios.create({
  timeout: 5000
});

/* =========================
   OPENAI
========================= */
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

function safeJSONParse(str) {
  try {
    return JSON.parse(str);
  } catch {
    const match = str.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
    throw new Error('Invalid JSON from model');
  }
}

async function parseBookingMessage(userMessage) {
  const response = await openai.chat.completions.create({
    model: 'gpt-4o-mini',
    messages: [
      {
        role: 'system',
        content: `Return ONLY JSON:
{
  "intent": "book" | "view" | "cancel" | "unknown",
  "court": "A" | "B" | null,
  "date": "YYYY-MM-DD" | null,
  "start_time": "HH:MM" | null,
  "end_time": "HH:MM" | null,
  "name": string | null,
  "house_number": "###" | null
}`
      },
      { role: 'user', content: userMessage }
    ]
  });

  const raw = response.choices[0].message.content;
  return safeJSONParse(raw);
}

/* =========================
   GOOGLE SHEETS
========================= */
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY),
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const sheets = google.sheets({ version: 'v4', auth });

async function getExistingBookings() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: 'Sheet1!A:H'
  });

  const rows = res.data.values || [];

  return rows.slice(1).map(row => ({
    timestamp: row[0],
    intent: row[1],
    name: row[2],
    house_number: row[3],
    court: row[4],
    date: row[5],
    start_time: row[6],
    end_time: row[7]
  }));
}

async function appendToSheet(data) {
  const values = [[
    new Date().toISOString(),
    data.intent,
    data.name,
    data.house_number,
    data.court,
    data.date,
    data.start_time,
    data.end_time
  ]];

  await sheets.spreadsheets.values.append({
    spreadsheetId: GOOGLE_SHEET_ID,
    range: 'Sheet1!A:H',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values }
  });
}

/* =========================
   TIME UTILS
========================= */
function toMinutes(t) {
  if (!t) return null;
  const [h, m] = t.split(':').map(Number);
  return h * 60 + m;
}

function isValidTimeRange(start, end) {
  const s = toMinutes(start);
  const e = toMinutes(end);
  return s !== null && e !== null && s < e;
}

function hasConflict(newBooking, existingBookings) {
  const ns = toMinutes(newBooking.start_time);
  const ne = toMinutes(newBooking.end_time);

  return existingBookings.some(b => {
    if (
      b.intent !== 'book' ||
      b.court !== newBooking.court ||
      b.date !== newBooking.date
    ) return false;

    const es = toMinutes(b.start_time);
    const ee = toMinutes(b.end_time);

    return ns < ee && ne > es;
  });
}

/* =========================
   MESSENGER
========================= */
async function sendMessage(psid, text) {
  await axiosClient.post(
    `https://graph.facebook.com/v18.0/me/messages?access_token=${PAGE_ACCESS_TOKEN}`,
    {
      recipient: { id: psid },
      message: { text }
    }
  );
}

/* =========================
   HEALTH CHECK
========================= */
app.get('/', (req, res) => {
  res.status(200).send('OK');
});

/* =========================
   WEBHOOK VERIFY
========================= */
app.get('/webhook', (req, res) => {
  if (
    req.query['hub.mode'] === 'subscribe' &&
    req.query['hub.verify_token'] === VERIFY_TOKEN
  ) {
    return res.status(200).send(req.query['hub.challenge']);
  }
  res.sendStatus(403);
});

/* =========================
   MAIN HANDLER (FAST ACK)
========================= */
app.post('/webhook', (req, res) => {
  res.sendStatus(200); // immediate response for Meta
  handleWebhook(req.body).catch(console.error);
});

/* =========================
   CORE LOGIC
========================= */
async function handleWebhook(body) {
  if (body.object !== 'page') return;

  for (const entry of body.entry) {
    const event = entry.messaging[0];

    if (!event.message?.text) continue;

    const psid = event.sender.id;
    const userMessage = event.message.text;

    let parsed;
    try {
      parsed = await parseBookingMessage(userMessage);
    } catch (err) {
      await sendMessage(psid, 'Could not understand your request.');
      continue;
    }

    if (parsed.intent === 'book') {

      // Normalize
      parsed.court = parsed.court?.toUpperCase();
      parsed.house_number = Number(parsed.house_number);

      // Validate
      if (
        !parsed.court ||
        !parsed.date ||
        !parsed.start_time ||
        !parsed.end_time ||
        !parsed.name ||
        isNaN(parsed.house_number)
      ) {
        await sendMessage(psid, 'Some details are missing or invalid. Please include court, date, time, name, and house number.');
        continue;
      }

      if (!isValidTimeRange(parsed.start_time, parsed.end_time)) {
        await sendMessage(psid, 'Invalid time range.');
        continue;
      }

      // Fetch existing bookings
      const existing = await getExistingBookings();

      // Conflict check
      if (hasConflict(parsed, existing)) {
        await sendMessage(
          psid,
          `❌ Time slot unavailable for Court ${parsed.court} on ${parsed.date}.`
        );
        continue;
      }

      // Write booking
      await appendToSheet(parsed);

      // Post-write verification (best-effort)
      const updated = await getExistingBookings();
      const conflictsNow = updated.filter(b =>
        b.court === parsed.court &&
        b.date === parsed.date &&
        toMinutes(parsed.start_time) < toMinutes(b.end_time) &&
        toMinutes(parsed.end_time) > toMinutes(b.start_time)
      );

      if (conflictsNow.length > 1) {
        await sendMessage(psid, '⚠️ Booking recorded, but potential conflict detected. Admin will review.');
        continue;
      }

      await sendMessage(
        psid,
        `✅ Booking confirmed:
Court ${parsed.court}
${parsed.date}
${parsed.start_time} - ${parsed.end_time}`
      );

    } else {
      await sendMessage(psid, `Parsed:\n${JSON.stringify(parsed, null, 2)}`);
    }
  }
}

/* =========================
   START (Cloud Run)
========================= */
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Running on port ${PORT}`);
});