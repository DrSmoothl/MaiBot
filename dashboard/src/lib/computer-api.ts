import { backendApi } from '@/lib/http'

const root = '/api/webui/computer'

export interface ComputerStatus {
  enabled: boolean
  paused: boolean
  takeover: boolean
  sandbox_running: boolean
  pending_request: string | null
  error: string
}

interface OperationResult {
  success: boolean
  status: string
  operation_id: string
  error?: string
  capture_error?: string
}

export const getComputerStatus = (signal: AbortSignal) =>
  backendApi.get<ComputerStatus>(root, { signal })

export const getComputerFrame = (signal: AbortSignal) =>
  backendApi.get<Blob>(`${root}/frame`, { signal, parse: 'blob', cache: 'no-store' })

export const controlComputer = (operation: 'pause' | 'takeover' | 'resume' | 'stop' | 'restart', signal: AbortSignal) =>
  backendApi.post<ComputerStatus>(`${root}/control`, { body: { operation }, signal })

export const inputComputer = (actions: Record<string, unknown>[], signal: AbortSignal) =>
  backendApi.post<OperationResult>(`${root}/input`, { body: { actions }, signal })

export const pollComputerOperation = (id: string, signal: AbortSignal) =>
  backendApi.get<OperationResult>(`${root}/operations/${encodeURIComponent(id)}`, { signal })
