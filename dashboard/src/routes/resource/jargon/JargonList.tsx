import React from 'react'
import { Check, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, Edit, Eye, Globe, HelpCircle, Trash2, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Input } from '@/components/ui/input'
import { ThinkingIllustration } from '@/components/ui/thinking-illustration'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { cn } from '@/lib/utils'

import type { Jargon } from '@/types/jargon'

interface JargonListProps {
  jargons: Jargon[]
  loading: boolean
  total: number
  page: number
  pageSize: number
  selectedIds: Set<number>
  hideChatColumn?: boolean
  className?: string
  onEdit: (jargon: Jargon) => void
  onViewDetail: (jargon: Jargon) => void
  onDelete: (jargon: Jargon) => void
  onToggleSelect: (id: number) => void
  onToggleSelectAll: () => void
  onPageChange: (page: number) => void
  onJumpToPage: (page: string) => void
}

/**
 * 渲染黑话状态徽章
 */
function renderJargonStatus(isJargon: boolean | null) {
  if (isJargon === true) {
    return <Badge variant="default" className="bg-green-600 hover:bg-green-700"><Check className="h-3 w-3 mr-1" />是黑话</Badge>
  } else if (isJargon === false) {
    return <Badge variant="secondary"><X className="h-3 w-3 mr-1" />非黑话</Badge>
  } else {
    return <Badge variant="outline"><HelpCircle className="h-3 w-3 mr-1" />未判定</Badge>
  }
}

function renderCreatedBy(createdBy: Jargon['created_by']) {
  return createdBy === 'MANUAL' ? (
    <Badge variant="outline">手动</Badge>
  ) : (
    <Badge variant="secondary">AI</Badge>
  )
}

function formatJargonChatDisplay(jargon: Jargon) {
  const chatNames = jargon.chat_names?.length ? jargon.chat_names : []
  if (chatNames.length > 0) {
    return chatNames.join('、')
  }
  return jargon.chat_name || jargon.session_id
}

/**
 * 黑话列表组件（桁面端表格 + 移动端卡片 + 分页）
 */
export function JargonList({
  jargons,
  loading,
  total,
  page,
  pageSize,
  selectedIds,
  hideChatColumn = false,
  className,
  onEdit,
  onViewDetail,
  onDelete,
  onToggleSelect,
  onToggleSelectAll,
  onPageChange,
  onJumpToPage,
}: JargonListProps) {
  const [jumpToPage, setJumpToPage] = React.useState('')
  const tableColSpan = hideChatColumn ? 6 : 7

  const handleJumpToPage = () => {
    onJumpToPage(jumpToPage)
    setJumpToPage('')
  }

  return (
    <div className={cn('flex min-h-0 flex-col rounded-lg border bg-card', className)}>
      {/* 桁面端表格视图 */}
      <div className="hidden min-h-0 flex-1 overflow-auto md:block">
        <Table aria-label="黑话列表">
          <TableHeader>
            <TableRow>
              <TableHead className="w-12">
                <Checkbox
                  checked={selectedIds.size === jargons.length && jargons.length > 0}
                  onCheckedChange={onToggleSelectAll}
                />
              </TableHead>
              <TableHead>内容</TableHead>
              <TableHead>含义</TableHead>
              {!hideChatColumn && <TableHead>聊天</TableHead>}
              <TableHead>状态</TableHead>
              <TableHead className="text-center">遇见次数</TableHead>
              <TableHead className="text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow>
                <TableCell colSpan={tableColSpan} className="text-center py-8 text-muted-foreground">
                  <ThinkingIllustration size="sm" className="mx-auto" />
                </TableCell>
              </TableRow>
            ) : jargons.length === 0 ? (
              <TableRow>
                <TableCell colSpan={tableColSpan} className="text-center py-8 text-muted-foreground">
                  暂无数据
                </TableCell>
              </TableRow>
            ) : (
              jargons.map((jargon) => (
                <TableRow key={jargon.id}>
                  <TableCell>
                    <Checkbox
                      checked={selectedIds.has(jargon.id)}
                      onCheckedChange={() => onToggleSelect(jargon.id)}
                    />
                  </TableCell>
                  <TableCell className="font-medium max-w-[200px]">
                    <div className="flex items-center gap-2">
                      {jargon.is_global && <span title="全局黑话"><Globe className="h-4 w-4 text-blue-500 flex-shrink-0" /></span>}
                      <span className="truncate" title={jargon.content}>{jargon.content}</span>
                    </div>
                  </TableCell>
                  <TableCell className="max-w-[260px]" title={jargon.meaning || ''}>
                    {jargon.meaning ? (
                      <span className="line-clamp-3 whitespace-normal break-words leading-5">
                        {jargon.meaning}
                      </span>
                    ) : (
                      <span className="text-muted-foreground">-</span>
                    )}
                  </TableCell>
                  {!hideChatColumn && (
                    <TableCell className="max-w-[220px]" title={formatJargonChatDisplay(jargon)}>
                      <span className="line-clamp-3 whitespace-normal break-words leading-5">
                        {formatJargonChatDisplay(jargon)}
                      </span>
                    </TableCell>
                  )}
                  <TableCell>
                    <div className="flex flex-wrap gap-1">
                      {renderJargonStatus(jargon.is_jargon)}
                      {renderCreatedBy(jargon.created_by)}
                    </div>
                  </TableCell>
                  <TableCell className="text-center">{jargon.count}</TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-2">
                      <Button
                        variant="default"
                        size="icon"
                        className="h-8 w-8"
                        onClick={() => onEdit(jargon)}
                        title="编辑"
                      >
                        <Edit className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="outline"
                        size="icon"
                        className="h-8 w-8"
                        onClick={() => onViewDetail(jargon)}
                        title="查看详情"
                      >
                        <Eye className="h-4 w-4" />
                      </Button>
                      <Button
                        size="sm"
                        onClick={() => onDelete(jargon)}
                        className="bg-red-600 hover:bg-red-700 text-white"
                      >
                        <Trash2 className="h-4 w-4 mr-1" />
                        删除
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {/* 移动端卡片视图 */}
      <div className="md:hidden space-y-3 p-4">
        {loading ? (
          <div className="text-center py-8 text-muted-foreground">
            <ThinkingIllustration size="sm" className="mx-auto" />
          </div>
        ) : jargons.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">暂无数据</div>
        ) : (
          jargons.map((jargon) => (
            <div key={jargon.id} className="rounded-lg border bg-card p-4 space-y-3">
              <div className="flex items-start gap-3">
                <Checkbox
                  checked={selectedIds.has(jargon.id)}
                  onCheckedChange={() => onToggleSelect(jargon.id)}
                  className="mt-1"
                />
                <div className="min-w-0 flex-1 space-y-2">
                  <div className="flex items-center gap-2">
                    {jargon.is_global && <Globe className="h-4 w-4 text-blue-500 flex-shrink-0" />}
                    <h3 className="font-semibold text-sm break-all">{jargon.content}</h3>
                  </div>
                  {jargon.meaning && (
                    <p className="text-sm text-muted-foreground break-all">{jargon.meaning}</p>
                  )}
                  <div className="flex flex-wrap items-center gap-2 text-xs">
                    {renderJargonStatus(jargon.is_jargon)}
                    {renderCreatedBy(jargon.created_by)}
                    <span className="text-muted-foreground">次数: {jargon.count}</span>
                  </div>
                  {!hideChatColumn && (
                    <div className="text-xs text-muted-foreground truncate">
                      聊天: {formatJargonChatDisplay(jargon)}
                    </div>
                  )}
                </div>
              </div>
              <div className="flex flex-wrap gap-1 pt-2 border-t">
                <Button variant="outline" size="sm" onClick={() => onEdit(jargon)} className="text-xs px-2 py-1 h-auto">
                  <Edit className="h-3 w-3 mr-1" />编辑
                </Button>
                <Button variant="outline" size="sm" onClick={() => onViewDetail(jargon)} className="text-xs px-2 py-1 h-auto">
                  <Eye className="h-3 w-3" />
                </Button>
                <Button variant="outline" size="sm" onClick={() => onDelete(jargon)} className="text-xs px-2 py-1 h-auto text-destructive hover:text-destructive">
                  <Trash2 className="h-3 w-3 mr-1" />删除
                </Button>
              </div>
            </div>
          ))
        )}
      </div>

      {/* 分页 */}
      {total > 0 && (
        <div className="flex flex-col sm:flex-row items-center justify-between gap-4 px-4 py-3 border-t">
          <div className="text-sm text-muted-foreground">
            共 {total} 条记录，第 {page} / {Math.ceil(total / pageSize)} 页
          </div>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={() => onPageChange(1)} disabled={page === 1} className="hidden sm:flex">
              <ChevronsLeft className="h-4 w-4" />
            </Button>
            <Button variant="outline" size="sm" onClick={() => onPageChange(page - 1)} disabled={page === 1}>
              <ChevronLeft className="h-4 w-4 sm:mr-1" />
              <span className="hidden sm:inline">上一页</span>
            </Button>
            <div className="flex items-center gap-2">
              <Input
                type="number"
                value={jumpToPage}
                onChange={(e) => setJumpToPage(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleJumpToPage()}
                placeholder={page.toString()}
                className="w-16 h-8 text-center"
                min={1}
                max={Math.ceil(total / pageSize)}
              />
              <Button variant="outline" size="sm" onClick={handleJumpToPage} disabled={!jumpToPage} className="h-8">
                跳转
              </Button>
            </div>
            <Button variant="outline" size="sm" onClick={() => onPageChange(page + 1)} disabled={page >= Math.ceil(total / pageSize)}>
              <span className="hidden sm:inline">下一页</span>
              <ChevronRight className="h-4 w-4 sm:ml-1" />
            </Button>
            <Button variant="outline" size="sm" onClick={() => onPageChange(Math.ceil(total / pageSize))} disabled={page >= Math.ceil(total / pageSize)} className="hidden sm:flex">
              <ChevronsRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
