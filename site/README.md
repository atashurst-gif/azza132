# Candidate Summary Builder

A static site that turns pasted candidate notes into the Candidate Summary PDF,
laid out to match the existing template (black page, neon banner, pink
punctuation, Name / Candidate Summary / Cost sections).

Everything runs in the browser — no server, no upload, no API key. The notes
never leave the machine they were pasted on.

## Deploying to Netlify

The repo root already has `netlify.toml` pointing at this folder:

```toml
[build]
  publish = "site"
```

- **From Git:** connect the repo in Netlify and deploy. No build command, no
  Node version to pin.
- **By drag and drop:** drop this `site/` folder onto the Netlify dashboard.
- **From the CLI:** `netlify deploy --prod --dir=site`

## Using it

1. Paste the notes into the big box (the usual `Client: … Position: … Name: …`
   block) and press **Build document** (or Ctrl/Cmd+Enter).
2. The PDF renders in the preview pane on the right.
3. Open **Document fields** to adjust anything before downloading — the name,
   the composed Candidate Summary, and the Cost line are all editable, and edits
   re-render the preview as you type.
4. Press **Download PDF**. The file is named
   `<Name> - Candidate Summary.pdf`.

### What the notes turn into

| Note field | Where it lands |
| --- | --- |
| `Name` | Banner title and the `Name:` section |
| `Personality`, `Age`, `Hobbies` | Opening paragraph of the summary |
| `Relevant Experience` | Second paragraph, near-verbatim |
| `Suitability` + `Position` | "He/She is well suited to the … role, due to …" |
| `WiFi`, `Accent`, `Availability`, `Other` | Closing practical paragraph |
| `Cost` | The `Cost:` section (omitted when blank) |
| `Client`, `Current Salary`, `Pronounced` | Only shown if "list the raw detail fields" is ticked |

Pronouns (he / she / they) are taken from the wording of the notes themselves.
Common note typos are cleaned up: `" ,"` spacing, a stray prefix glued to a
pronoun (`laHe has…` → `He has…`), and `His not married` → `He is not married`.

Anything the composer gets wrong is fixable in the **Candidate Summary** box
before downloading.

## Files

| File | Purpose |
| --- | --- |
| `layout.js` | Page geometry — every number measured from the reference PDF |
| `parse.js` | Notes → fields → composed summary prose |
| `pdf.js` | Draws the document with pdf-lib |
| `app.js` | Wiring for the page |
| `assets/header.jpg` | The neon banner from the template |
| `fonts/` | Document typeface (see below) |
| `vendor/` | pdf-lib 1.17.1 and @pdf-lib/fontkit 1.1.1, bundled so there is no CDN dependency |

## Typeface

The original document is set in **Neue Montreal**, a commercial typeface that
cannot be redistributed here, so the site ships with
[Hanken Grotesk](https://fonts.google.com/specimen/Hanken+Grotesk) (SIL Open
Font License) — the closest open match in shape and weight. It is about 9%
wider than Neue Montreal, so the text is drawn with a matching horizontal
scale: line breaks, baselines and page count come out the same as the
original document.

If you have a Neue Montreal licence, drop the file in as
`fonts/NeueMontreal-Regular.otf` (or `.ttf`) and redeploy — it is picked up
automatically and drawn unscaled, so the output then matches the template
glyph for glyph. (Until then the browser console logs two harmless 404s from
looking for it.)

Notes longer than one page continue onto plain black pages with the same
margins and spacing.

## Changing the banner

Either replace `assets/header.jpg` and redeploy, or use the **Replace banner
image** picker for a one-off document.
