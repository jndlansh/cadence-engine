/**
 * Task output capture policy.
 *
 * A task's stdout/stderr is what makes a finished (or failed) run debuggable
 * after the fact — before this, the output was discarded the moment the process
 * exited. But it cannot be allowed to grow without bound: it travels in
 * `cadence:completions` entries and is persisted in the workflow state hash,
 * both of which are read by everything downstream. So capture is *bounded*, and
 * a cut capture says so in the text rather than silently looking complete.
 *
 * Two roles, deliberately separate:
 * - the **producer** (the worker) cuts output to {@link MAX_PERSISTED_OUTPUT_CHARS}
 *   and appends a marker naming how much it dropped;
 * - the **receiver** (the reducer that persists it) stores anything inside the
 *   conforming envelope untouched, and only cuts a value that no conforming
 *   producer could have written.
 *
 * That split matters: re-cutting an already-marked capture would replace the
 * honest "N more characters" count with a smaller, misleading one. Live
 * verification caught exactly that.
 */

/**
 * Characters kept per stream — stdout and stderr are bounded separately, so a
 * chatty stdout cannot crowd out the stderr that explains a failure. Counted in
 * UTF-16 code units (`String.length`), which is close enough for log text.
 */
export const MAX_PERSISTED_OUTPUT_CHARS = 4096;

/**
 * Slack allowed on top of the capture limit for the marker the producer appends.
 * A received value within `MAX_PERSISTED_OUTPUT_CHARS + this` is already bounded
 * and must be stored verbatim.
 */
export const OUTPUT_MARKER_ALLOWANCE = 128;

/** The marker appended to a cut capture. */
function truncationMarker(omitted: number): string {
  return `…[output truncated: ${omitted} more characters]`;
}

/**
 * Bound an output string, keeping the **head** and marking the cut explicitly.
 *
 * The head is kept because it shows what the command set out to do; the failure
 * detail is what `error`/`stderr` carry. The marker is part of the returned text
 * so a reader cannot mistake a truncated capture for a complete one.
 */
export function truncateOutput(
  text: string,
  max: number = MAX_PERSISTED_OUTPUT_CHARS
): string {
  if (text.length <= max) return text;
  return `${text.slice(0, max)}${truncationMarker(text.length - max)}`;
}

/**
 * Prepare a received capture for persistence. Anything a conforming producer
 * could have written (bounded output plus its marker) is returned unchanged;
 * only a value beyond that envelope is cut, which means it came from a
 * non-conforming writer and its marker — if any — was not trustworthy anyway.
 */
export function boundReceivedOutput(text: string | undefined): string | undefined {
  if (text === undefined) return undefined;
  const envelope = MAX_PERSISTED_OUTPUT_CHARS + OUTPUT_MARKER_ALLOWANCE;
  return text.length <= envelope ? text : truncateOutput(text, envelope);
}
