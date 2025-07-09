import { cpus } from 'os'

/**
 * Get the number of CPU cores available on the system.
 *
 * @returns The number of CPU cores, or 1 if the system cannot determine it.
 */
export function numberOfCpus(): number {
  return cpus().length || 1
}
