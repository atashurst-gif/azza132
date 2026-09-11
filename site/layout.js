/*
 * layout.js — exact metrics lifted from the reference document
 * (Kauthar_Noor.pdf). Every number below was measured from that PDF's
 * text baselines, so output is a pixel-for-pixel match of the template.
 */
export const PAGE_W = 595.5;
export const PAGE_H = 842.25;

export const HEADER_H = 254.3458;

export const MARGIN_L = 81.04;
export const RIGHT_EDGE = 543.0;
export const CONTENT_W = RIGHT_EDGE - MARGIN_L;

export const PINK = { r: 0xe4 / 255, g: 0x1c / 255, b: 0xbe / 255 };
export const WHITE = { r: 1, g: 1, b: 1 };

// Banner text (baselines measured from the top of the page)
export const KICKER_SIZE = 9.7;
export const KICKER_BASE = 186.15;
export const NAME_SIZE = 19.9;
export const NAME_BASE = 220.09;

// Centred document heading
export const HEADING_SIZE = 18.5;
export const HEADING_BASE = 288.29;

// Body flow
export const LABEL_SIZE = 17.5;
export const BODY_SIZE = 12.0;
export const FIRST_LABEL_BASE = 336.66; // first section label on page 1
export const LABEL_TO_VALUE = 20.0; // label baseline -> single-line value
export const LABEL_TO_BODY = 32.15; // label baseline -> first paragraph line
export const LINE_LEADING = 16.505; // line to line inside a paragraph
export const PARA_GAP = 33.01; // last line of para -> first line of next
export const VALUE_TO_LABEL = 38.18; // value baseline -> next section label
export const PARA_TO_LABEL = 45.32; // last paragraph line -> next section label

/*
 * The fallback typeface is about 9% wider than the Neue Montreal of the
 * reference document, so text is drawn with a horizontal scale (PDF `Tz`)
 * that brings the advance widths back in line. That keeps line breaks — and
 * therefore the page count — the same as the original. Set to 1 when the
 * real Neue Montreal is installed in fonts/.
 */
export const CONDENSE = 0.916;
export const CONDENSE_NEUE_MONTREAL = 1;

// Continuation pages
export const CONT_TOP_BASE = 100.0;
// Deepest baseline allowed on a page; the reference sets its last line at 768.4.
export const BOTTOM_LIMIT = PAGE_H - 66;
