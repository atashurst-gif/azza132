import { parseFields, composeSummary, firstName, DETAIL_FIELDS, tidy } from './parse.js';
import { buildPdf, gapFor } from './pdf.js';

const $ = (id) => document.getElementById(id);
const els = {
  notes: $('notes'),
  build: $('build'),
  sample: $('sample'),
  clear: $('clear'),
  tweaks: $('tweaks'),
  kicker: $('kicker'),
  name: $('name'),
  heading: $('heading'),
  cost: $('cost'),
  summary: $('summary'),
  details: $('details'),
  banner: $('banner'),
  preview: $('preview'),
  status: $('status'),
  download: $('download'),
};

const EXAMPLE = `Client: ReGen
Position: Sales Lead Generator (B2C)
Name: Divan Cloete
Pronounced:
Age: 32
Hobbies: Hiking, watching sports and trying different food at restaurants
Personality: Confident, Motivated and Mature
Current Salary: R 12 000 basic and commission structure uncapped, he would make 8000 commission
Relevant Experience: He has over 4 years of sales and customer service experience in high-volume BPO environments across US campaigns. He has 3+ years of telesales experience, regularly making 250-400 outbound calls daily, with strong skills in sales conversion, objection handling, and lead follow-up. His background includes both B2C and B2B sales, as well as customer service roles in healthcare and life insurance, where he developed strong empathy and communication skills when assisting clients with sensitive matters. He is also experienced in CRM management, multi-system environments, and pipeline tracking, making him well suited for a fast-paced lead generation role.
Suitability: Yes, due to his proven ability to thrive in high-volume outbound calling environments, his strong sales background, and his ability to engage with clients in a professional and empathetic manner.
WiFi: 41 mbps, Laptop, headphones and backup for load shedding
Accent: Good
Availability: ASAP
Other: His not married and does not have children`;

let fields = {};
let bannerBytes = null;
let pdfUrl = null;
let renderToken = 0;

function setStatus(text, kind) {
  els.status.textContent = text;
  els.status.dataset.kind = kind || '';
}

/** Notes -> form fields. */
function populateFromNotes() {
  fields = parseFields(els.notes.value);
  els.name.value = tidy(fields.Name || '');
  els.cost.value = tidy(fields.Cost || '');
  els.summary.value = composeSummary(fields);
  els.tweaks.open = true;
}

/** Form fields -> the section list the renderer draws. */
function buildDoc() {
  const sections = [];
  const push = (label, type, text) => {
    if (!String(text || '').trim()) return;
    const prev = sections[sections.length - 1];
    sections.push({ label, type, text: String(text).trim(), gapBefore: gapFor(prev && prev.type) });
  };

  push('Name', 'value', els.name.value);
  push('Candidate Summary', 'paras', els.summary.value);
  if (els.details.checked) {
    for (const key of DETAIL_FIELDS) push(key, 'value', fields[key]);
  }
  push('Cost', 'value', els.cost.value);

  return {
    kicker: els.kicker.value.trim(),
    name: els.name.value.trim(),
    heading: els.heading.value.trim(),
    sections,
    headerImage: bannerBytes,
  };
}

function fileName() {
  const base = (els.name.value.trim() || 'Candidate') + ' - ' + (els.heading.value.trim() || 'Summary');
  return base.replace(/[\\/:*?"<>|]+/g, '').replace(/\s+/g, ' ') + '.pdf';
}

async function render() {
  const token = ++renderToken;
  setStatus('Rendering…');
  try {
    const bytes = await buildPdf(buildDoc());
    if (token !== renderToken) return; // a newer render already ran
    if (pdfUrl) URL.revokeObjectURL(pdfUrl);
    pdfUrl = URL.createObjectURL(new Blob([bytes], { type: 'application/pdf' }));
    els.preview.src = pdfUrl;
    els.download.disabled = false;
    setStatus('Ready — ' + fileName(), 'ok');
  } catch (err) {
    console.error(err);
    els.download.disabled = true;
    setStatus('Could not build the PDF: ' + err.message, 'error');
  }
}

let debounce;
function renderSoon() {
  clearTimeout(debounce);
  debounce = setTimeout(render, 250);
}

els.build.addEventListener('click', () => {
  if (!els.notes.value.trim()) {
    setStatus('Paste the candidate notes first.', 'error');
    return;
  }
  populateFromNotes();
  render();
});

els.sample.addEventListener('click', () => {
  els.notes.value = EXAMPLE;
  populateFromNotes();
  render();
});

els.clear.addEventListener('click', () => {
  els.notes.value = '';
  fields = {};
  for (const id of ['name', 'cost', 'summary']) els[id].value = '';
  els.download.disabled = true;
  els.preview.removeAttribute('src');
  setStatus('Paste the notes and press “Build document”.');
});

for (const id of ['kicker', 'name', 'heading', 'cost', 'summary']) {
  els[id].addEventListener('input', () => {
    if (!els.download.disabled || els.summary.value.trim()) renderSoon();
  });
}
els.details.addEventListener('change', renderSoon);

els.banner.addEventListener('change', async () => {
  const file = els.banner.files && els.banner.files[0];
  bannerBytes = file ? await file.arrayBuffer() : null;
  renderSoon();
});

els.download.addEventListener('click', () => {
  if (!pdfUrl) return;
  const a = document.createElement('a');
  a.href = pdfUrl;
  a.download = fileName();
  document.body.appendChild(a);
  a.click();
  a.remove();
});

// Ctrl/Cmd+Enter in the notes box builds the document.
els.notes.addEventListener('keydown', (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') els.build.click();
});
