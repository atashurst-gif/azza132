/*
 * parse.js — turns the pasted candidate notes into the fields and the
 * composed "Candidate Summary" prose used by the document.
 */

// Field labels recognised at the start of a line. Order matters only for
// the optional "details" block.
export const FIELD_ORDER = [
  'Client',
  'Position',
  'Name',
  'Pronounced',
  'Age',
  'Hobbies',
  'Personality',
  'Current Salary',
  'Relevant Experience',
  'Suitability',
  'WiFi',
  'Accent',
  'Availability',
  'Cost',
  'Other',
];

const ALIASES = {
  'wi-fi': 'WiFi',
  wifi: 'WiFi',
  'internet': 'WiFi',
  'salary': 'Current Salary',
  'current salary': 'Current Salary',
  'experience': 'Relevant Experience',
  'relevant experience': 'Relevant Experience',
  'pronunciation': 'Pronounced',
  'pronounced': 'Pronounced',
  'candidate': 'Name',
  'full name': 'Name',
  'role': 'Position',
  'position': 'Position',
  'client': 'Client',
  'name': 'Name',
  'age': 'Age',
  'hobbies': 'Hobbies',
  'personality': 'Personality',
  'suitability': 'Suitability',
  'accent': 'Accent',
  'availability': 'Availability',
  'cost': 'Cost',
  'other': 'Other',
  'notes': 'Other',
};

/** Split the pasted text into { field: value }. Unlabelled continuation
 *  lines are appended to the field above them. */
export function parseFields(text) {
  const out = {};
  let current = null;
  for (const rawLine of String(text || '').split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }
    const m = line.match(/^([A-Za-z][A-Za-z\-/ ]{1,24}?)\s*:\s*(.*)$/);
    const key = m && ALIASES[m[1].trim().toLowerCase()];
    if (key) {
      current = key;
      out[key] = m[2].trim();
    } else if (current) {
      out[current] = (out[current] ? out[current] + ' ' : '') + line;
    }
  }
  for (const k of Object.keys(out)) out[k] = tidy(out[k]);
  return out;
}

/** Normalise the spacing quirks that come out of the notes (" ," etc.). */
export function tidy(s) {
  return String(s || '')
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;:])/g, '$1')
    .replace(/([,;])(?=\S)/g, '$1 ')
    .replace(/\.(?=[A-Z])/g, '. ')
    .trim();
}

function endSentence(s) {
  const t = tidy(s);
  if (!t) return '';
  return /[.!?]$/.test(t) ? t : t + '.';
}

function lower1(s) {
  return s ? s.charAt(0).toLowerCase() + s.slice(1) : s;
}

/** "laptop" -> "a laptop"; plurals and phrases are left alone. */
function withArticle(item) {
  const t = tidy(item);
  if (!t || /^(a|an|the|some)\b/i.test(t)) return t;
  if (/s$/i.test(t.split(' ')[0])) return t; // headphones, backups...
  return (/^[aeiou]/i.test(t) ? 'an ' : 'a ') + t;
}

/** Oxford-comma list: ["a","b","c"] -> "a, b, and c" */
export function listJoin(items) {
  const xs = items.filter(Boolean);
  if (xs.length === 0) return '';
  if (xs.length === 1) return xs[0];
  if (xs.length === 2) return `${xs[0]} and ${xs[1]}`;
  return `${xs.slice(0, -1).join(', ')}, and ${xs[xs.length - 1]}`;
}

/** Split a free-form list on commas, slashes and a trailing "and". */
export function splitList(s) {
  return tidy(s)
    .split(/\s*(?:,|\/|\band\b)\s*/i)
    .map((x) => x.replace(/^[-–•]\s*/, '').trim())
    .filter(Boolean);
}

/** Work out which pronouns to write with, from the notes themselves. */
export function detectPronouns(fields) {
  const blob = Object.values(fields).join(' ').toLowerCase();
  const male = (blob.match(/\b(he|him|his)\b/g) || []).length;
  const female = (blob.match(/\b(she|her|hers)\b/g) || []).length;
  if (male > female) return { subj: 'He', obj: 'him', poss: 'his', plural: false };
  if (female > male) return { subj: 'She', obj: 'her', poss: 'her', plural: false };
  return { subj: 'They', obj: 'them', poss: 'their', plural: true };
}

export function firstName(fields) {
  return tidy(fields.Name || '').split(' ')[0] || 'The candidate';
}

/** Small, safe clean-ups for the typos that recur in the notes. */
function fixStart(text) {
  let t = tidy(text);
  // stray keystrokes glued to the front of a pronoun: "laHe has..." -> "He has..."
  t = t.replace(/^[a-z]{1,3}(?=(He|She|They|His|Her|Their)\b)/, '');
  // "His not married" -> "He is not married"
  t = t.replace(/^His (?=(not|currently|also|still|single)\b)/i, 'He is ');
  t = t.replace(/^Hers (?=(not|currently|also|still|single)\b)/i, 'She is ');
  return t.trim();
}

/** Strip a leading "Yes,"/"Yes -"/"No," verdict, returning the reason. */
function stripVerdict(text) {
  const t = tidy(text);
  const m = t.match(/^(yes|no)\b[\s,.\-–—]*(.*)$/i);
  if (!m) return { verdict: null, reason: t };
  return { verdict: m[1].toLowerCase(), reason: m[2].trim() };
}

/**
 * Compose the Candidate Summary paragraphs, following the reference
 * document: an opening impression, the experience, the suitability
 * verdict, then the practical details.
 */
export function composeSummary(fields) {
  const p = detectPronouns(fields);
  const name = firstName(fields);
  const paras = [];

  // 1. Opening impression — personality, optionally age and hobbies.
  const traits = splitList(fields.Personality || '').map(lower1);
  const opening = [];
  if (traits.length) {
    opening.push(`${name} comes across as ${listJoin(traits)}.`);
  }
  const bits = [];
  if (fields.Age) bits.push(`is ${tidy(fields.Age).replace(/\s*(years old|yrs?)$/i, '')} years old`);
  if (fields.Hobbies) bits.push(`enjoys ${listJoin(splitList(fields.Hobbies).map(lower1))}`);
  if (bits.length) {
    opening.push(`${p.subj} ${listJoin(bits)}.`);
  }
  if (opening.length) paras.push(opening.join(' '));

  // 2. Relevant experience — used close to verbatim.
  if (fields['Relevant Experience']) {
    paras.push(endSentence(fixStart(fields['Relevant Experience'])));
  }

  // 3. Suitability.
  if (fields.Suitability) {
    const { verdict, reason } = stripVerdict(fields.Suitability);
    const role = tidy(fields.Position || '');
    if (verdict === 'no') {
      const tail = reason ? ` ${lower1(endSentence(reason))}` : '';
      paras.push(
        endSentence(
          `${p.subj} ${p.plural ? 'are' : 'is'} not suited to the ${role || 'role'}` +
            (tail ? `,${tail.replace(/\.$/, '')}` : '')
        )
      );
    } else {
      const lead = `${p.subj} ${p.plural ? 'are' : 'is'} well suited to the ${
        role ? role + ' role' : 'role'
      }`;
      const tail = reason ? ` ${lower1(reason).replace(/^due to/i, 'due to')}` : '';
      paras.push(endSentence(tail ? `${lead},${tail}` : lead));
    }
  }

  // 4. Practical details — setup, accent, availability, anything else.
  const practical = [];
  if (fields.WiFi) {
    const items = splitList(fields.WiFi);
    const speed = items.find((x) => /\d.*(mb|mbps|mb\/s)/i.test(x));
    const parts = items
      .filter((x) => x !== speed)
      .map((x) => withArticle(lower1(x)));
    if (speed) parts.unshift(`a ${tidy(speed)} internet connection`);
    practical.push(
      `${p.subj} ${p.plural ? 'have' : 'has'} ${listJoin(parts) || tidy(fields.WiFi)}.`
    );
  }
  const closing = [];
  if (fields.Accent) {
    const acc = lower1(tidy(fields.Accent));
    closing.push(
      `${p.plural ? 'have' : 'has'} ${/\baccent\b/i.test(acc) ? acc : 'a ' + acc + ' accent'}`
    );
  }
  if (fields.Availability) {
    closing.push(`${p.plural ? 'are' : 'is'} available to start ${tidy(fields.Availability)}`);
  }
  if (closing.length) practical.push(`${p.subj} ${listJoin(closing)}.`);
  if (fields.Other) practical.push(endSentence(fixStart(fields.Other)));
  if (practical.length) paras.push(practical.join(' '));

  return paras.join('\n\n');
}

/** Sections that appear under the summary when "include detail fields" is on. */
export const DETAIL_FIELDS = [
  'Client',
  'Position',
  'Pronounced',
  'Age',
  'Hobbies',
  'Personality',
  'Current Salary',
  'WiFi',
  'Accent',
  'Availability',
  'Other',
];
