import { backendApi } from '@/lib/http'

const root = '/api/webui/computer'

export interface ComputerStatus {
  enabled: boolean
  paused: boolean
  takeover: boolean
  sandbox_running: boolean
  agent_state: string
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

export const controlComputer = (operation: 'pause' | 'takeover' | 'resume' | 'stop', signal: AbortSignal) =>
  backendApi.post<ComputerStatus>(`${root}/control`, { body: { operation }, signal })

export const inputComputer = (actions: Record<string, unknown>[], signal: AbortSignal) =>
  backendApi.post<OperationResult>(`${root}/input`, { body: { actions }, signal })

export const pollComputerOperation = (id: string, signal: AbortSignal) =>
  backendApi.get<OperationResult>(`${root}/operations/${encodeURIComponent(id)}`, { signal })

export interface ComputerEvent {
  backend?: string
  index: number
  id: string
  time: string
  kind: string
  items: Array<{
    item_type: string
    parts?: Array<{ type: string; text?: string }>
    tool_call?: { func_name: string; args: unknown }
    output?: string
    success?: boolean
    tool_name?: string
    display_kind?: string
    summary_parts?: string[]
    text_parts?: string[]
  }>
}

export const getComputerEvents = (after: number, signal: AbortSignal) =>
  backendApi.get<{ events: ComputerEvent[]; queued: ComputerEvent[]; state: string; error: string; pending: boolean; backend: string }>(`${root}/events`, { query: { after }, signal })

export const sendComputerMessage = (text: string) =>
  backendApi.post<{ id: string; backend: string }>(`${root}/message`, { body: { text } })
