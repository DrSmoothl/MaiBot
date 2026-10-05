import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { PluginWebUIRenderer } from '@/components/plugin-webui-renderer'
import { resolveNodeValue } from '@/lib/plugin-webui'
import type { WebUINode } from '@/lib/plugin-webui'

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }))

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

describe('plugin WebUI renderer', () => {
  it('renders plugin text as text without interpreting HTML', () => {
    const content = '<img src=x onerror=alert(1)>'
    const { container } = render(
      <PluginWebUIRenderer
        nodes={[node({ value: content })]}
        data={{}}
        values={{}}
        busy={false}
        onChange={vi.fn()}
        onAction={vi.fn()}
      />
    )
    expect(screen.getByText(content)).toBeInTheDocument()
    expect(container.querySelector('img')).toBeNull()
  })

  it('resolves only own data fields and exposes invalid bindings', () => {
    const reference = node({ value: { source: 'summary', field: 'totals.count' } })
    expect(resolveNodeValue(reference, { summary: { totals: { count: 4 } } })).toBe(4)
    expect(() => resolveNodeValue(reference, { summary: {} })).toThrow('Missing data field')
    expect(() =>
      resolveNodeValue(node({ value: { source: 'summary', field: 'toString' } }), { summary: {} })
    ).toThrow('Missing data field')
  })

  it('dispatches declared actions and disables buttons during requests', () => {
    const onAction = vi.fn()
    const nodes = [node({ type: 'button', label: 'Recalculate', action: 'recalculate' })]
    const { rerender } = render(
      <PluginWebUIRenderer
        nodes={nodes}
        data={{}}
        values={{}}
        busy={false}
        onChange={vi.fn()}
        onAction={onAction}
      />
    )
    fireEvent.click(screen.getByRole('button', { name: 'Recalculate' }))
    expect(onAction).toHaveBeenCalledWith('recalculate')
    rerender(
      <PluginWebUIRenderer
        nodes={nodes}
        data={{}}
        values={{}}
        busy
        onChange={vi.fn()}
        onAction={onAction}
      />
    )
    expect(screen.getByRole('button', { name: 'Recalculate' })).toBeDisabled()
  })

  it('paginates tables instead of rendering unbounded result rows', () => {
    const rows = Array.from({ length: 60 }, (_, i) => ({ name: `row-${i}` }))
    render(
      <PluginWebUIRenderer
        nodes={[
          node({
            type: 'table',
            value: { source: 'rows', field: '' },
            columns: [{ field: 'name', label: 'Name' }],
          }),
        ]}
        data={{ rows }}
        values={{}}
        busy={false}
        onChange={vi.fn()}
        onAction={vi.fn()}
      />
    )
    expect(screen.getByText('row-0')).toBeInTheDocument()
    expect(screen.queryByText('row-50')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'pluginWebUI.next' }))
    expect(screen.getByText('row-50')).toBeInTheDocument()
    expect(screen.queryByText('row-0')).not.toBeInTheDocument()
  })
})
