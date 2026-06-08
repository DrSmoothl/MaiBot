import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { AMemorixRetrievalChatsHook } from '../AMemorixRetrievalChatsHook'
import { AMemorixRetrievalFilterMirrorHook } from '../AMemorixRetrievalFilterMirrorHook'
import {
  buildAMemorixRetrievalChatTokenOptions,
  resolveAMemorixRetrievalFilterMirrorKind,
  resolveAMemorixRetrievalChatsCopy,
} from '../AMemorixRetrievalChatsHook.utils'
import type { MemoryImportChatTargetPayload } from '@/lib/memory-api'

vi.mock('@/lib/memory-api', () => ({
  getMemoryImportChatTargets: vi.fn(async () => ({
    success: true,
    data: [
      {
        chat_id: 'session-group',
        chat_name: '测试群',
        platform: 'qq',
        group_id: '10001',
        user_id: '',
        is_group: true,
      },
      {
        chat_id: 'session-private',
        chat_name: '小明的私聊',
        platform: 'qq',
        group_id: '',
        user_id: '20002',
        is_group: false,
      },
    ],
  })),
}))

describe('AMemorixRetrievalChatsHook', () => {
  it('builds stream, group, private and user token options from chat targets', () => {
    const options = buildAMemorixRetrievalChatTokenOptions([
      {
        chat_id: 'session-group',
        chat_name: '测试群',
        platform: 'qq',
        group_id: '10001',
        user_id: '',
        is_group: true,
      },
      {
        chat_id: 'session-private',
        chat_name: '小明的私聊',
        platform: 'qq',
        group_id: '',
        user_id: '20002',
        is_group: false,
      },
    ] as MemoryImportChatTargetPayload[])

    expect(options.map((item) => item.token)).toEqual([
      'stream:session-group',
      'stream:session-private',
      'group:10001',
      'user:20002',
      'private:20002',
    ])
  })

  it('lets users add manual tokens when editing retrieval filter chats', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()

    render(
      <AMemorixRetrievalChatsHook
        fieldPath="a_memorix.filter.retrieval.chat_summary.chats"
        onChange={onChange}
        schema={{
          name: 'chats',
          type: 'array',
          label: '聊天流列表',
          description: '聊天流列表',
          required: false,
        }}
        value={['stream:session-group']}
      />,
    )

    await waitFor(() => {
      expect(screen.getByLabelText('选择 group:10001')).toBeInTheDocument()
    })

    await user.type(screen.getByPlaceholderText('手动添加，如 group:123 或 stream:session_id'), 'group:10001')
    await user.click(screen.getByRole('button', { name: '添加' }))

    expect(onChange).toHaveBeenLastCalledWith(['stream:session-group', 'group:10001'])
  })

  it('uses distinct labels for each retrieval result type', () => {
    expect(resolveAMemorixRetrievalChatsCopy('a_memorix.filter.retrieval.chat_stream.chats').title)
      .toBe('普通聊天流记忆过滤范围')
    expect(resolveAMemorixRetrievalChatsCopy('a_memorix.filter.retrieval.chat_summary.chats').title)
      .toBe('聊天总结记忆过滤范围')
    expect(resolveAMemorixRetrievalChatsCopy('a_memorix.filter.retrieval.episode.chats').title)
      .toBe('Episode 记忆过滤范围')
  })

  it('maps normal setting sections to their retrieval filter kinds', () => {
    expect(resolveAMemorixRetrievalFilterMirrorKind('a_memorix.retrieval')).toBe('chat_stream')
    expect(resolveAMemorixRetrievalFilterMirrorKind('a_memorix.integration')).toBe('chat_summary')
    expect(resolveAMemorixRetrievalFilterMirrorKind('a_memorix.episode')).toBe('episode')
  })

  it('writes mirrored section controls back to the shared retrieval filter config', async () => {
    const user = userEvent.setup()
    const onParentChange = vi.fn()

    render(
      <AMemorixRetrievalFilterMirrorHook
        fieldPath="a_memorix.episode"
        onParentChange={onParentChange}
        parentValues={{
          filter: {
            retrieval: {
              episode: {
                enabled: false,
                mode: 'blacklist',
                chats: [],
              },
            },
          },
        }}
        value={{}}
      >
        <div>Episode 原有设置</div>
      </AMemorixRetrievalFilterMirrorHook>,
    )

    expect(screen.getByText('Episode 原有设置')).toBeInTheDocument()
    expect(screen.getByText('检索结果过滤范围')).toBeInTheDocument()
    expect(screen.getByText('未启用，黑名单，已选择 0 个聊天流 token。')).toBeInTheDocument()
    expect(screen.queryByText('Episode 记忆过滤范围')).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: '展开检索结果过滤范围' }))

    expect(screen.getByText('Episode 记忆过滤范围')).toBeInTheDocument()
    await user.click(screen.getByRole('switch'))

    expect(onParentChange).toHaveBeenCalledWith('filter.retrieval.episode.enabled', true)
  })
})
