import type { ReactNode } from 'react'

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AdapterHostPolicyPanel, AUTOSAVE_DELAY_MS } from './AdapterHostPolicyPanel'
import type { AdapterHostPolicy, AdapterPolicyDefaults } from '@/lib/chat-management-api'
import { getAdapterHostPolicy, updateAdapterHostPolicy } from '@/lib/chat-management-api'

const { toastMock } = vi.hoisted(() => ({ toastMock: vi.fn() }))

vi.mock('@/hooks/use-toast', () => ({ useToast: () => ({ toast: toastMock }) }))

vi.mock('@/lib/chat-management-api', () => ({
  getAdapterHostPolicy: vi.fn(),
  updateAdapterHostPolicy: vi.fn(),
}))

vi.mock('@/components/ListFieldEditor', () => ({
  ListFieldEditor: ({
    onChange,
    placeholder,
    value,
  }: {
    onChange?: (next: unknown[]) => void
    placeholder?: string
    value?: unknown[]
  }) => {
    const items = Array.isArray(value) ? value : []
    const label = placeholder ?? ''
    return (
      <div data-placeholder={label} data-testid={`list-field:${label}`}>
        <span data-testid={`list-value:${label}`}>{JSON.stringify(items)}</span>
        <button type="button" onClick={() => onChange?.([...items, 'new-item'])}>
          {`添加:${label}`}
        </button>
        <button type="button" onClick={() => onChange?.([123, true])}>
          {`设为混合类型:${label}`}
        </button>
        <button
          type="button"
          onClick={() => {
            items.push('mutated-in-place')
            onChange?.(items)
          }}
        >
          {`就地修改:${label}`}
        </button>
        <button type="button" onClick={() => onChange?.([])}>
          {`清空:${label}`}
        </button>
      </div>
    )
  },
}))

vi.mock('@/components/ui/select', async () => {
  const React = await import('react')
  const SelectContext = React.createContext<{
    value?: string
    onValueChange?: (value: string) => void
  }>({})

  function Select({
    value,
    onValueChange,
    children,
  }: {
    value?: string
    onValueChange?: (value: string) => void
    children?: ReactNode
  }) {
    return (
      <SelectContext.Provider value={{ value, onValueChange }}>
        <div data-testid="host-policy-select" data-value={value}>
          {children}
        </div>
      </SelectContext.Provider>
    )
  }

  function SelectTrigger({ children }: { children?: ReactNode }) {
    return <div>{children}</div>
  }

  function SelectValue() {
    const { value } = React.useContext(SelectContext)
    const labels: Record<string, string> = {
      inherit: '默认',
      block: '不阅读',
      allow: '阅读',
    }
    return <span>{value ? (labels[value] ?? value) : null}</span>
  }

  function SelectContent({ children }: { children?: ReactNode }) {
    return <div>{children}</div>
  }

  function SelectItem({ value, children }: { value: string; children?: ReactNode }) {
    const { onValueChange } = React.useContext(SelectContext)
    return (
      <button type="button" data-value={value} onClick={() => onValueChange?.(value)}>
        {children}
      </button>
    )
  }

  return { Select, SelectContent, SelectItem, SelectTrigger, SelectValue }
})

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, reject, resolve }
}

function makePolicy(overrides: Partial<{
  group: Partial<AdapterHostPolicy['group']>
  private: Partial<AdapterHostPolicy['private']>
}> = {}): AdapterHostPolicy {
  return {
    group: {
      default_action: 'inherit',
      allow_ids: [],
      deny_ids: [],
      ...overrides.group,
    },
    private: {
      default_action: 'inherit',
      allow_ids: [],
      deny_ids: [],
      ...overrides.private,
    },
  }
}

function makeResponse(
  pluginId = 'adapter.qq',
  overrides: {
    global_defaults?: AdapterPolicyDefaults
    policy?: AdapterHostPolicy
  } = {}
) {
  return {
    success: true,
    plugin_id: pluginId,
    global_defaults: overrides.global_defaults ?? { group: 'allow' as const, private: 'block' as const },
    policy: overrides.policy ?? makePolicy(),
  }
}

function makeQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
}

function renderPanel(pluginId = 'adapter.qq', queryClient = makeQueryClient()) {
  const view = render(
    <QueryClientProvider client={queryClient}>
      <AdapterHostPolicyPanel pluginId={pluginId} />
    </QueryClientProvider>
  )
  return { ...view, queryClient }
}

async function renderReadyPanel(
  pluginId = 'adapter.qq',
  response = makeResponse(pluginId),
  queryClient = makeQueryClient()
) {
  vi.mocked(getAdapterHostPolicy).mockResolvedValue(response as never)
  const view = renderPanel(pluginId, queryClient)
  expect(await screen.findByTestId('list-field:输入需要阅读的群号')).toBeInTheDocument()
  return { ...view, response }
}

beforeEach(() => {
  vi.mocked(getAdapterHostPolicy).mockResolvedValue(makeResponse() as never)
  vi.mocked(updateAdapterHostPolicy).mockResolvedValue(makeResponse() as never)
})

afterEach(() => {
  vi.clearAllMocks()
})

describe('AdapterHostPolicyPanel', () => {
  it('加载中展示转圈提示', async () => {
    const deferred = createDeferred<ReturnType<typeof makeResponse>>()
    vi.mocked(getAdapterHostPolicy).mockReturnValue(deferred.promise as never)
    renderPanel()

    expect(await screen.findByText('正在加载主程序规则')).toBeInTheDocument()
    deferred.resolve(makeResponse())
    expect(await screen.findByTestId('list-field:输入需要阅读的群号')).toBeInTheDocument()
  })

  it('加载失败时展示 Error.message', async () => {
    vi.mocked(getAdapterHostPolicy).mockRejectedValue(new Error('策略服务不可用'))
    renderPanel()
    expect(await screen.findByText('策略服务不可用')).toBeInTheDocument()
  })

  it('加载失败且非 Error 时使用回退文案', async () => {
    vi.mocked(getAdapterHostPolicy).mockRejectedValue('offline')
    renderPanel()
    expect(await screen.findByText('主程序规则加载失败')).toBeInTheDocument()
  })

  it('查询成功但没有 data 时使用回退文案', async () => {
    vi.mocked(getAdapterHostPolicy).mockResolvedValue(null as never)
    renderPanel()
    expect(await screen.findByText('主程序规则加载失败')).toBeInTheDocument()
  })

  it('成功渲染群聊/私聊规则、全局默认和列表占位符', async () => {
    await renderReadyPanel()

    expect(screen.queryByText('这是 MaiBot 主程序侧规则，与适配器自身名单相互独立。')).not.toBeInTheDocument()
    expect(screen.queryByText(/适配器自身的白名单仍在/)).not.toBeInTheDocument()
    expect(screen.getByText('群聊规则')).toBeInTheDocument()
    expect(screen.getByText('私聊规则')).toBeInTheDocument()
    expect(screen.getByText('全局默认：阅读')).toBeInTheDocument()
    expect(screen.getByText('全局默认：不阅读')).toBeInTheDocument()
    expect(screen.getAllByText('群聊填写群号，私聊填写用户 ID。')).toHaveLength(2)
    expect(screen.getAllByText('同一 ID 不可同时出现在阅读和不阅读列表。')).toHaveLength(2)
    expect(screen.getByTestId('list-field:输入需要阅读的群号')).toBeInTheDocument()
    expect(screen.getByTestId('list-field:输入不阅读的群号')).toBeInTheDocument()
    expect(screen.getByTestId('list-field:输入需要阅读的用户 ID')).toBeInTheDocument()
    expect(screen.getByTestId('list-field:输入不阅读的用户 ID')).toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: '默认' })).toHaveLength(2)
    expect(screen.getAllByRole('button', { name: '不阅读' })).toHaveLength(2)
    expect(screen.getAllByRole('button', { name: '阅读' })).toHaveLength(2)
  })

  it('未做修改时不会触发自动保存，保存按钮禁用', async () => {
    await renderReadyPanel()
    await new Promise((resolve) => setTimeout(resolve, AUTOSAVE_DELAY_MS + 200))
    expect(updateAdapterHostPolicy).not.toHaveBeenCalled()
    expect(screen.queryByTestId('host-policy-save-status')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: '保存' })).toBeDisabled()
  })

  it('点击保存按钮绕过防抖立即写回', async () => {
    const user = userEvent.setup()
    await renderReadyPanel()

    await user.click(screen.getAllByRole('button', { name: '阅读' })[0])
    await user.click(screen.getByRole('button', { name: '保存' }))

    await waitFor(
      () =>
        expect(updateAdapterHostPolicy).toHaveBeenCalledWith('adapter.qq', {
          group: { default_action: 'allow', allow_ids: [], deny_ids: [] },
          private: { default_action: 'inherit', allow_ids: [], deny_ids: [] },
        }),
      { timeout: 6000 }
    )
  })

  it('修改群聊默认规则后停止编辑即自动保存并展示保存时间', async () => {
    const user = userEvent.setup()
    vi.mocked(updateAdapterHostPolicy).mockResolvedValue(
      makeResponse('adapter.qq', { policy: makePolicy({ group: { default_action: 'allow' } }) }) as never
    )
    await renderReadyPanel()

    expect(screen.queryByTestId('host-policy-save-status')).not.toBeInTheDocument()
    await user.click(screen.getAllByRole('button', { name: '阅读' })[0])
    expect(await screen.findByTestId('host-policy-save-status')).toHaveTextContent('未保存的更改')

    await waitFor(
      () =>
        expect(updateAdapterHostPolicy).toHaveBeenCalledWith('adapter.qq', {
          group: { default_action: 'allow', allow_ids: [], deny_ids: [] },
          private: { default_action: 'inherit', allow_ids: [], deny_ids: [] },
        }),
      { timeout: 6000 }
    )
    expect(getAdapterHostPolicy).toHaveBeenCalledWith('adapter.qq')
    expect(await screen.findByTestId('host-policy-save-status')).toHaveTextContent('已保存')
  })

  it('修改私聊名单时把非字符串项转成字符串', async () => {
    const user = userEvent.setup()
    await renderReadyPanel()

    await user.click(screen.getByRole('button', { name: '设为混合类型:输入需要阅读的用户 ID' }))
    await user.click(screen.getByRole('button', { name: '添加:输入不阅读的用户 ID' }))

    await waitFor(
      () =>
        expect(updateAdapterHostPolicy).toHaveBeenCalledWith('adapter.qq', {
          group: { default_action: 'inherit', allow_ids: [], deny_ids: [] },
          private: {
            default_action: 'inherit',
            allow_ids: ['123', 'true'],
            deny_ids: ['new-item'],
          },
        }),
      { timeout: 6000 }
    )
  })

  it('就地修改列表时 clonePolicy 会隔离初始数据', async () => {
    const user = userEvent.setup()
    const allowIds = ['g1']
    const policy = makePolicy({ group: { allow_ids: allowIds } })
    await renderReadyPanel('adapter.qq', makeResponse('adapter.qq', { policy }))

    await user.click(screen.getByRole('button', { name: '就地修改:输入需要阅读的群号' }))

    expect(allowIds).toEqual(['g1'])
    expect(screen.getByTestId('list-value:输入需要阅读的群号')).toHaveTextContent(
      JSON.stringify(['g1', 'mutated-in-place'])
    )
    await waitFor(
      () => expect(updateAdapterHostPolicy).toHaveBeenCalled(),
      { timeout: 6000 }
    )
  })

  it('自动保存成功后写入 query cache、失效聊天流详情，并热重载草稿', async () => {
    const user = userEvent.setup()
    const queryClient = makeQueryClient()
    const setQueryData = vi.spyOn(queryClient, 'setQueryData')
    const invalidateQueries = vi.spyOn(queryClient, 'invalidateQueries')
    const savedPolicy = makePolicy({
      group: { default_action: 'block', deny_ids: ['999'] },
    })
    const saved = makeResponse('adapter.qq', { policy: savedPolicy })
    vi.mocked(updateAdapterHostPolicy).mockResolvedValue(saved as never)
    await renderReadyPanel('adapter.qq', makeResponse('adapter.qq'), queryClient)

    await user.click(screen.getAllByRole('button', { name: '不阅读' })[0])
    await user.click(screen.getByRole('button', { name: '添加:输入不阅读的群号' }))

    await waitFor(
      () => expect(setQueryData).toHaveBeenCalledWith(['adapter-host-policy', 'adapter.qq'], saved),
      { timeout: 6000 }
    )
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['chat-stream-detail'] })
    await waitFor(() =>
      expect(screen.getByTestId('host-policy-save-status')).toHaveTextContent('已保存')
    )
    // 回包落缓存后草稿被热重载为服务端返回的名单
    expect(screen.getByTestId('list-value:输入不阅读的群号')).toHaveTextContent(
      JSON.stringify(['999'])
    )
  })

  it('自动保存失败时按 Error / 非 Error 弹出 toast 并展示失败状态', async () => {
    const user = userEvent.setup()
    await renderReadyPanel()

    vi.mocked(updateAdapterHostPolicy).mockRejectedValueOnce(new Error('写入失败'))
    await user.click(screen.getAllByRole('button', { name: '阅读' })[0])
    await waitFor(
      () =>
        expect(toastMock).toHaveBeenCalledWith({
          title: '主程序放行规则保存失败',
          description: '写入失败',
          variant: 'destructive',
        }),
      { timeout: 6000 }
    )
    expect(screen.getByTestId('host-policy-save-status')).toHaveTextContent('保存失败')

    vi.mocked(updateAdapterHostPolicy).mockRejectedValueOnce('offline')
    await user.click(screen.getByRole('button', { name: '添加:输入不阅读的群号' }))
    await waitFor(
      () =>
        expect(toastMock).toHaveBeenCalledWith({
          title: '主程序放行规则保存失败',
          description: '请稍后重试',
          variant: 'destructive',
        }),
      { timeout: 6000 }
    )
  })

  it('自动保存进行中展示保存状态', async () => {
    const user = userEvent.setup()
    const deferred = createDeferred<ReturnType<typeof makeResponse>>()
    vi.mocked(updateAdapterHostPolicy).mockReturnValue(deferred.promise as never)
    await renderReadyPanel()

    await user.click(screen.getAllByRole('button', { name: '阅读' })[1])

    await waitFor(
      () => expect(screen.getByTestId('host-policy-save-status')).toHaveTextContent('自动保存中'),
      { timeout: 6000 }
    )

    deferred.resolve(makeResponse())
    await waitFor(
      () => expect(screen.getByTestId('host-policy-save-status')).toHaveTextContent('已保存'),
      { timeout: 6000 }
    )
  })

  it('pluginId 变化会按新 id 重新拉取规则', async () => {
    const queryClient = makeQueryClient()
    vi.mocked(getAdapterHostPolicy).mockImplementation(async (pluginId: string) => {
      return makeResponse(pluginId, {
        policy: makePolicy({
          group: { allow_ids: pluginId === 'adapter.telegram' ? ['tg'] : ['qq'] },
        }),
      }) as never
    })
    const { rerender } = renderPanel('adapter.qq', queryClient)
    expect(await screen.findByTestId('list-value:输入需要阅读的群号')).toHaveTextContent(
      JSON.stringify(['qq'])
    )

    rerender(
      <QueryClientProvider client={queryClient}>
        <AdapterHostPolicyPanel pluginId="adapter.telegram" />
      </QueryClientProvider>
    )

    await waitFor(() =>
      expect(getAdapterHostPolicy).toHaveBeenCalledWith('adapter.telegram')
    )
    expect(await screen.findByTestId('list-value:输入需要阅读的群号')).toHaveTextContent(
      JSON.stringify(['tg'])
    )
  })
})
