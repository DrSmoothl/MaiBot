import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { WebUIExtension, WebUIPage, WebUINode } from '@/lib/plugin-webui'
import { PluginWebUIPage } from '@/routes/plugin-webui'

const mocks = vi.hoisted(() => ({ extensions: [] as WebUIExtension[], invoke: vi.fn() }))
vi.mock('@tanstack/react-router', () => ({
  useParams: () => ({ pluginId: 'test.plugin', pageId: 'overview' }),
  Link: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
}))
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }))
vi.mock('@/lib/plugin-webui', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/plugin-webui')>()),
  usePluginWebUI: () => ({
    extensions: mocks.extensions,
    loading: false,
    error: null,
    preferences: { hidden: [], order: [] },
  }),
  invokePluginWebUI: mocks.invoke,
}))

function node(overrides: Partial<WebUINode>): WebUINode {
  return {
    type: 'text',
    label: null,
    value: null,
    children: [],
    columns: null,
    name: null,
    options: [],
    action: null,
    variant: 'primary',
    chart_type: 'line',
    x: null,
    y: null,
    ...overrides,
  }
}
function page(overrides: Partial<WebUIPage> = {}): WebUIPage {
  return {
    id: 'overview',
    title: 'Overview',
    description: '',
    placement: 'workspace',
    icon: 'puzzle',
    queries: {
      summary: { api: 'summary', version: '1', parameters: {}, confirmation: null },
    },
    actions: {},
    content: [node({ type: 'stat', label: 'Total', value: { source: 'summary', field: 'count' } })],
    ...overrides,
  }
}
function install(value: WebUIPage) {
  mocks.extensions = [{ plugin_id: 'test.plugin', workspace_title: 'Statistics', pages: [value] }]
}

describe('plugin page lifecycle', () => {
  beforeEach(() => {
    mocks.invoke.mockResolvedValue({ count: 3 })
    install(page())
  })

  it('keeps inputs usable when a required query argument has not been filled', async () => {
    install(
      page({
        queries: {
          summary: {
            api: 'summary',
            version: '1',
            confirmation: null,
            parameters: {
              term: {
                type: 'string',
                required: true,
                max_length: 100,
                minimum: null,
                maximum: null,
                choices: [],
              },
            },
          },
        },
        content: [
          node({ type: 'input', label: 'Search', name: 'term' }),
          node({ type: 'stat', value: { source: 'summary', field: 'count' } }),
        ],
      })
    )
    mocks.invoke.mockImplementation((_plugin, _page, _kind, _name, args) =>
      args.term ? Promise.resolve({ count: 3 }) : Promise.reject(new Error('Missing term'))
    )
    render(<PluginWebUIPage />)
    expect(await screen.findByText('Error: Missing term')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Search'), { target: { value: 'hello' } })
    fireEvent.click(screen.getByRole('button', { name: 'pluginWebUI.refresh' }))
    expect(await screen.findByText('3')).toBeInTheDocument()
    expect(mocks.invoke).toHaveBeenLastCalledWith(
      'test.plugin',
      'overview',
      'queries',
      'summary',
      { term: 'hello' },
      false,
      expect.any(AbortSignal)
    )
  })

  it('requires host confirmation before dispatching a declared write action', async () => {
    install(
      page({
        actions: {
          reset: { api: 'reset', version: '1', parameters: {}, confirmation: 'Reset all counts?' },
        },
        content: [node({ type: 'button', label: 'Reset', action: 'reset', variant: 'danger' })],
      })
    )
    render(<PluginWebUIPage />)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Reset' })).not.toBeDisabled())
    fireEvent.click(screen.getByRole('button', { name: 'Reset' }))
    expect(screen.getByText('Reset all counts?')).toBeInTheDocument()
    expect(mocks.invoke.mock.calls.filter((call) => call[2] === 'actions')).toHaveLength(0)
    fireEvent.click(screen.getByRole('button', { name: 'pluginWebUI.confirm' }))
    await waitFor(() =>
      expect(mocks.invoke).toHaveBeenCalledWith(
        'test.plugin',
        'overview',
        'actions',
        'reset',
        {},
        true,
        expect.any(AbortSignal)
      )
    )
    expect(await screen.findByText('pluginWebUI.completed')).toBeInTheDocument()
  })

  it('aborts the browser request on unmount without retrying the write', async () => {
    install(
      page({
        queries: {},
        actions: { run: { api: 'run', version: '1', parameters: {}, confirmation: null } },
        content: [node({ type: 'button', label: 'Run', action: 'run' })],
      })
    )
    mocks.invoke.mockImplementation(() => new Promise(() => {}))
    const view = render(<PluginWebUIPage />)
    await waitFor(() => expect(screen.getByRole('button', { name: 'Run' })).not.toBeDisabled())
    fireEvent.click(screen.getByRole('button', { name: 'Run' }))
    const signal = mocks.invoke.mock.calls[0][6] as AbortSignal
    view.unmount()
    expect(signal.aborted).toBe(true)
    expect(mocks.invoke).toHaveBeenCalledOnce()
  })
})
