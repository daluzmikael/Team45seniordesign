/**
 * Subtle grayscale basketball motifs behind chat content (pointer-events none).
 * Every SVG in /public/background_img/simple is listed below — sync from
 * ../../background_img/simple when adding assets.
 */
const BASE = "/background_img/simple"

/** Light: darker glyphs on matte gray. Dark: invert so black artwork reads as light on dark surfaces. */
const matte = {
  strong:
    "opacity-[0.22] grayscale contrast-100 dark:invert dark:opacity-[0.26]",
  mid: "opacity-[0.19] grayscale contrast-100 dark:invert dark:opacity-[0.23]",
  soft: "opacity-[0.16] grayscale contrast-100 dark:invert dark:opacity-[0.20]",
  faint:
    "opacity-[0.13] grayscale contrast-100 dark:invert dark:opacity-[0.17]",
  whisper:
    "opacity-[0.11] grayscale contrast-100 dark:invert dark:opacity-[0.14]",
  panorama:
    "opacity-[0.14] grayscale contrast-100 dark:invert dark:opacity-[0.18]",
} as const

/** Order = paint order (later items sit on top). Positions spread to avoid clutter. */
const LAYERS: { file: string; className: string }[] = [
  {
    file: "basketball-court-2-svgrepo-com.svg",
    className: `absolute -left-6 top-4 w-[min(260px,42vw)] max-w-none ${matte.strong}`,
  },
  {
    file: "basketball-svgrepo-com.svg",
    className: `absolute -right-8 bottom-16 w-[min(280px,50vw)] max-w-none rotate-12 ${matte.mid}`,
  },
  {
    file: "basketball-goal-3-svgrepo-com.svg",
    className: `absolute -right-4 top-24 w-[min(140px,28vw)] max-w-none -rotate-6 ${matte.soft}`,
  },
  {
    file: "basketball-uniform-1-svgrepo-com.svg",
    className: `absolute left-[8%] top-[42%] w-[min(120px,22vw)] max-w-none rotate-[12deg] ${matte.faint}`,
  },
  {
    file: "basketball-6-svgrepo-com.svg",
    className: `absolute -left-4 bottom-32 w-[min(170px,32vw)] max-w-none ${matte.soft}`,
  },
  {
    file: "w29cS01.svg",
    className: `absolute bottom-24 left-[22%] w-[min(220px,40vw)] max-w-none ${matte.panorama}`,
  },
  {
    file: "zer7701.svg",
    className: `absolute bottom-[12%] left-0 w-[min(140px,26vw)] max-w-none -rotate-[5deg] ${matte.whisper}`,
  },
  {
    file: "54pQO01.svg",
    className: `absolute right-[6%] top-[28%] w-[min(150px,26vw)] max-w-none rotate-[6deg] ${matte.faint}`,
  },
  {
    file: "2Bmgc01.svg",
    className: `absolute left-1/2 top-[14%] w-[min(150px,27vw)] max-w-none -translate-x-1/2 rotate-[4deg] ${matte.whisper}`,
  },
  {
    file: "cepNp01.svg",
    className: `absolute bottom-[34%] right-[12%] w-[min(130px,23vw)] max-w-none -rotate-[8deg] ${matte.faint}`,
  },
]

export function BasketballDecorBackground() {
  return (
    <div
      aria-hidden
      className="pointer-events-none absolute inset-0 z-0 overflow-hidden select-none"
    >
      {LAYERS.map(({ file, className }) => (
        <img key={file} alt="" src={`${BASE}/${file}`} className={className} />
      ))}
    </div>
  )
}
