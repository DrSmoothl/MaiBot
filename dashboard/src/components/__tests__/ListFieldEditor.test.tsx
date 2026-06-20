import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { ListFieldEditor } from '@/components/ListFieldEditor'

describe('ListFieldEditor', () => {
  it('将 multiple=true 的 select 子字段渲染为多选下拉', async () => {
    const user = userEvent.setup()
    const handleChange = vi.fn()

    render(
      <ListFieldEditor
        value={[{ api_names: [] }]}
        onChange={handleChange}
        itemType="object"
        itemFields={{
          api_names: {
            type: 'select',
            label: '需要推送的 API 配置组',
            default: [],
            multiple: true,
            choices: ['daily_news', 'ai_news', 'it_news'],
          },
        }}
      />
    )

    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByText('ai_news'))

    expect(handleChange).toHaveBeenCalledWith([{ api_names: ['ai_news'] }])
  })

  it('对象数组中的多选子字段会将已选数字值规范为字符串并正确切换', async () => {
    const user = userEvent.setup()
    const handleChange = vi.fn()

    render(
      <ListFieldEditor
        value={[{ api_ids: [1] }]}
        onChange={handleChange}
        itemType="object"
        itemFields={{
          api_ids: {
            type: 'select',
            label: 'API 编号',
            default: [],
            multiple: true,
            choices: [1, 2, 3],
          },
        }}
      />
    )

    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getAllByText('1').at(-1)!)

    expect(handleChange).toHaveBeenCalledWith([{ api_ids: [] }])
  })

  it('父级 disabled 时禁用对象数组中的多选子字段', () => {
    render(
      <ListFieldEditor
        value={[{ api_names: ['daily_news'] }]}
        onChange={vi.fn()}
        itemType="object"
        itemFields={{
          api_names: {
            type: 'select',
            label: '需要推送的 API 配置组',
            default: [],
            multiple: true,
            choices: ['daily_news', 'ai_news', 'it_news'],
          },
        }}
        disabled
      />
    )

    expect(screen.getByRole('combobox')).toBeDisabled()
  })

  it('保留对象数组子字段中的嵌套字符串数组编辑能力', async () => {
    const handleChange = vi.fn()

    render(
      <ListFieldEditor
        value={[{ push_groups: ['group-a'] }]}
        onChange={handleChange}
        itemType="object"
        itemFields={{
          push_groups: {
            type: 'array',
            label: '推送群列表',
            default: [],
            item_type: 'string',
          },
        }}
      />
    )

    const input = screen.getByDisplayValue('group-a')
    fireEvent.change(input, { target: { value: 'group-b' } })

    expect(handleChange).toHaveBeenLastCalledWith([{ push_groups: ['group-b'] }])
  })
})
