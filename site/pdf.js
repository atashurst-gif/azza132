/*
 * pdf.js — draws the candidate summary document with pdf-lib, using the
 * measurements in layout.js so the result matches the reference PDF.
 */
import * as L from './layout.js';

/*
 * In the browser the libraries come from vendor/ and the assets over fetch.
 * `configure()` lets the test harness supply node equivalents instead.
 */
let deps = null;

export function configure(overrides) {
  deps = overrides;
}

function env() {
  if (deps) return deps;
  return {
    PDFLib: window.PDFLib,
    fontkit: window.fontkit,
    // Returns an ArrayBuffer, or null when the asset is not present.
    async loadAsset(url) {
      try {
        const res = await fetch(url);
        if (!res.ok) return null;
        const bytes = await res.arrayBuffer();
        return bytes.byteLength > 512 ? bytes : null;
      } catch (err) {
        return null;
      }
    },
  };
}

/*
 * The reference document is set in Neue Montreal, a commercial typeface we
 * cannot redistribute. Drop NeueMontreal-Regular.otf (or .ttf) into
 * site/fonts/ and it is used automatically; otherwise the very close
 * open-source Hanken Grotesk is used.
 */
const FONT_CANDIDATES = [
  'fonts/NeueMontreal-Regular.otf',
  'fonts/NeueMontreal-Regular.ttf',
  'fonts/HankenGrotesk-Regular.ttf',
];

let assetCache = null;

async function loadAssets() {
  if (assetCache) return assetCache;
  const { loadAsset } = env();
  const header = await loadAsset('assets/header.jpg');
  if (!header) throw new Error('assets/header.jpg is missing.');
  let font = null;
  let isNeueMontreal = false;
  for (const url of FONT_CANDIDATES) {
    font = await loadAsset(url);
    if (font) {
      isNeueMontreal = /NeueMontreal/i.test(url);
      break;
    }
  }
  if (!font) throw new Error('No document font found in fonts/.');
  assetCache = { header, font, isNeueMontreal };
  return assetCache;
}

/** Greedy word wrap against the real, drawn glyph widths. */
export function wrapText(text, measure, size, maxWidth) {
  const lines = [];
  for (const para of String(text).split(/\n/)) {
    const words = para.trim().split(/\s+/).filter(Boolean);
    if (!words.length) {
      lines.push('');
      continue;
    }
    let line = '';
    for (const word of words) {
      const next = line ? line + ' ' + word : word;
      if (measure(next, size) <= maxWidth || !line) {
        line = next;
      } else {
        lines.push(line);
        line = word;
      }
    }
    lines.push(line);
  }
  return lines;
}

/** Gap to put in front of a section, given the type of the one before it. */
export function gapFor(previousType) {
  return previousType === 'paras' ? L.PARA_TO_LABEL : L.VALUE_TO_LABEL;
}

/**
 * doc = {
 *   kicker, name, heading,
 *   sections: [{ label, type: 'value' | 'paras', text, gapBefore }],
 *   headerImage: ArrayBuffer | null
 * }
 * Returns the PDF bytes (Uint8Array).
 */
export async function buildPdf(doc) {
  const { PDFLib, fontkit } = env();
  const { PDFDocument, rgb } = PDFLib;
  const assets = await loadAssets();

  const pdf = await PDFDocument.create();
  pdf.registerFontkit(fontkit);
  const font = await pdf.embedFont(assets.font, { subset: true });
  const condense = assets.isNeueMontreal ? L.CONDENSE_NEUE_MONTREAL : L.CONDENSE;
  const measure = (text, size) => font.widthOfTextAtSize(text, size) * condense;
  const banner = await pdf.embedJpg(doc.headerImage || assets.header);

  pdf.setTitle(`${doc.name || 'Candidate'} - ${doc.heading || 'Candidate Summary'}`);
  pdf.setCreator('Candidate Summary Builder');
  pdf.setProducer('Candidate Summary Builder');

  const white = rgb(L.WHITE.r, L.WHITE.g, L.WHITE.b);
  const pink = rgb(L.PINK.r, L.PINK.g, L.PINK.b);

  let page = null;
  let baseline = 0;

  const newPage = (withBanner) => {
    page = pdf.addPage([L.PAGE_W, L.PAGE_H]);
    page.drawRectangle({ x: 0, y: 0, width: L.PAGE_W, height: L.PAGE_H, color: rgb(0, 0, 0) });
    if (condense !== 1) {
      // Horizontal scaling is part of the text state, so one operator per
      // page applies to every string drawn on it.
      page.pushOperators(PDFLib.PDFOperator.of('Tz', [PDFLib.PDFNumber.of(condense * 100)]));
    }
    if (withBanner) {
      page.drawImage(banner, {
        x: 0,
        y: L.PAGE_H - L.HEADER_H,
        width: L.PAGE_W,
        height: L.HEADER_H,
      });
    }
  };

  // `y` in these helpers is a baseline measured from the top of the page.
  const draw = (text, x, y, size, color) =>
    page.drawText(text, { x, y: L.PAGE_H - y, size, font, color });

  const centre = (text, y, size, color) =>
    draw(text, (L.PAGE_W - measure(text, size)) / 2, y, size, color);

  const ensureRoom = (needed) => {
    if (baseline + needed > L.BOTTOM_LIMIT) {
      newPage(false);
      baseline = L.CONT_TOP_BASE;
      return true;
    }
    return false;
  };

  // ---- page 1 banner ----
  newPage(true);
  if (doc.kicker) centre(doc.kicker, L.KICKER_BASE, L.KICKER_SIZE, pink);
  if (doc.name) centre(doc.name, L.NAME_BASE, L.NAME_SIZE, white);
  if (doc.heading) centre(doc.heading, L.HEADING_BASE, L.HEADING_SIZE, white);

  // ---- flowed sections ----
  baseline = L.FIRST_LABEL_BASE;
  let first = true;

  for (const section of doc.sections) {
    const label = (section.label || '').trim();
    const body = (section.text || '').trim();
    if (!label && !body) continue;

    const isParas = section.type === 'paras';
    if (!first) baseline += section.gapBefore != null ? section.gapBefore : L.VALUE_TO_LABEL;
    first = false;

    // Keep a label with at least its first line of text.
    ensureRoom(isParas ? L.LABEL_TO_BODY : L.LABEL_TO_VALUE);

    if (label) {
      draw(label, L.MARGIN_L, baseline, L.LABEL_SIZE, white);
      draw(
        ':',
        L.MARGIN_L + measure(label, L.LABEL_SIZE),
        baseline,
        L.LABEL_SIZE,
        pink
      );
      baseline += isParas ? L.LABEL_TO_BODY : L.LABEL_TO_VALUE;
    }

    const paragraphs = isParas ? body.split(/\n\s*\n/) : [body];
    paragraphs.forEach((para, pIndex) => {
      if (pIndex > 0) baseline += L.PARA_GAP - L.LINE_LEADING;
      wrapText(para, measure, L.BODY_SIZE, L.CONTENT_W).forEach((line, lIndex) => {
        if (pIndex > 0 || lIndex > 0) baseline += L.LINE_LEADING;
        ensureRoom(0);
        if (line) draw(line, L.MARGIN_L, baseline, L.BODY_SIZE, white);
      });
    });
  }

  return pdf.save();
}
