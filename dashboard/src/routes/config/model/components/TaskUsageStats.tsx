import { useEffect, useState } from 'react'

import { backendApi } from '@/lib/http'

interface TaskModelUsage {
  model_name: string
  model_identifier: string
  provider_name: string
  request_count: number
  failure_count: number
  avg_duration: number | null
  failure_rate: number
}

interface TaskUsage {
  task_name: string
  hours: number
  request_count: number
  models: TaskModelUsage[]
}

export function TaskUsageStats({ taskName }: { taskName: string }) {
  const [data, setData] = useState<TaskUsage | null>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    let active = true
    const load = async () => {
      setError('')
      try {
        const result = await backendApi.get<TaskUsage>('/api/webui/statistics/task-usage', {
          query: { task_name: taskName, hours: 24 },
          errorMessage: '获取任务使用情况失败',
        })
        if (active) setData(result)
      } catch (failure) {
        if (active) setError(failure instanceof Error ? failure.message : '获取任务使用情况失败')
      }
    }
    void load()
    const timer = window.setInterval(load, 30_000)
    return () => {
      active = false
      window.clearInterval(timer)
    }
  }, [taskName])

  const current = data?.task_name === taskName ? data : null
  return (
    <section className="mt-6 min-w-0 space-y-3 border-t-2 border-border py-4" aria-label="最近使用情况">
      {error ? (
        <p role="alert" className="text-destructive text-sm">{error}</p>
      ) : !current ? (
        <p role="status" className="text-muted-foreground text-sm">正在获取使用情况…</p>
      ) : (
        <>
          <p className="text-sm">最近 24 小时请求数 <span className="ml-2 text-xl font-semibold tabular-nums">{current.request_count.toLocaleString()}</span></p>
          {current.models.length === 0 ? (
            <p className="text-muted-foreground text-sm">最近 24 小时暂无调用记录。</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="sr-only">
                  <tr>
                    <th className="py-2 pr-4 font-medium">模型 / 提供商</th>
                    <th className="whitespace-nowrap px-3 py-2 text-right font-medium">平均耗时</th>
                    <th className="whitespace-nowrap py-2 pl-3 text-right font-medium">失败率</th>
                  </tr>
                </thead>
                <tbody>
                  {current.models.map((model) => (
                    <tr key={`${model.provider_name}/${model.model_name}/${model.model_identifier}`} className="border-b-2 border-border last:border-b-0">
                      <td className="py-3 pr-4">
                        <div className="break-all font-medium">{model.model_name}</div>
                        <div className="text-muted-foreground break-all text-xs">{model.provider_name}{model.model_identifier !== model.model_name ? ` · ${model.model_identifier}` : ''}</div>
                      </td>
                      <td className="whitespace-nowrap px-3 py-3 text-right tabular-nums">{model.avg_duration === null ? '—' : `${model.avg_duration.toFixed(2)} 秒`}</td>
                      <td className="whitespace-nowrap py-3 pl-3 text-right tabular-nums">{(model.failure_rate * 100).toFixed(1)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </section>
  )
}
