import { Hash, HelpCircle } from 'lucide-react'
import { useEffect, useState } from 'react'

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { MarkdownRenderer } from '@/components/markdown-renderer'
import { MultiSelect } from '@/components/ui/multi-select'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Textarea } from '@/components/ui/textarea'
import { useToast } from '@/hooks/use-toast'
import { cn } from '@/lib/utils'

import { createJargon, updateJargon } from '@/lib/jargon-api'

import type {
  Jargon,
  JargonChatInfo,
  JargonCreateRequest,
  JargonUpdateRequest,
} from '@/types/jargon'

// ====================
// 信息项组件
// ====================
function InfoItem({
  icon: Icon,
  label,
  value,
  mono = false,
}: {
  icon?: typeof Hash
  label: string
  value: string | null | undefined
  mono?: boolean
}) {
  return (
    <div className="space-y-1">
      <Label className="text-muted-foreground flex items-center gap-1 text-xs">
        {Icon && <Icon className="h-3 w-3" />}
        {label}
      </Label>
      <div className={cn('text-sm', mono && 'font-mono', !value && 'text-muted-foreground')}>
        {value || '-'}
      </div>
    </div>
  )
}

function formatJargonChatDisplay(jargon: Jargon) {
  const chatNames = jargon.chat_names?.length ? jargon.chat_names : []
  if (chatNames.length > 0) {
    return chatNames.join('、')
  }
  return jargon.chat_name || jargon.session_id
}

// ====================
// 黑话详情对话框
// ====================
interface JargonDetailDialogProps {
  jargon: Jargon | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function JargonDetailDialog({
  jargon,
  open,
  onOpenChange,
}: JargonDetailDialogProps) {
  if (!jargon) return null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="grid max-h-[80vh] max-w-2xl grid-rows-[auto_1fr_auto] overflow-hidden">
        <DialogHeader>
          <DialogTitle>黑话详情</DialogTitle>
          <DialogDescription>查看黑话的完整信息</DialogDescription>
        </DialogHeader>

        <DialogBody className="h-full">
          <div className="space-y-4 pb-2">
            <div className="grid grid-cols-2 gap-4">
              <InfoItem icon={Hash} label="记录ID" value={jargon.id.toString()} mono />
              <InfoItem label="使用次数" value={jargon.count.toString()} />
            </div>

            <div className="space-y-1">
              <Label className="text-muted-foreground text-xs">内容</Label>
              <div className="bg-muted rounded p-2 text-sm break-all whitespace-pre-wrap">
                {jargon.content}
              </div>
            </div>

            <div className="space-y-1">
              <Label className="text-muted-foreground text-xs">含义</Label>
              <div className="bg-muted rounded p-2 text-sm break-all">
                {jargon.meaning ? <MarkdownRenderer content={jargon.meaning} /> : '-'}
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <InfoItem label="聊天" value={formatJargonChatDisplay(jargon)} />
              <div className="space-y-1">
                <Label className="text-muted-foreground text-xs">状态</Label>
                <div className="flex items-center gap-2">
                  {jargon.is_jargon === true && (
                    <Badge variant="default" className="bg-green-600">
                      是黑话
                    </Badge>
                  )}
                  {jargon.is_jargon !== true && <Badge variant="secondary">无黑话</Badge>}
                  {jargon.is_legacy_empty_meaning && (
                    <Badge variant="outline">
                      <HelpCircle className="mr-1 h-3 w-3" />
                      旧数据
                    </Badge>
                  )}
                  {jargon.created_by === 'MANUAL' ? (
                    <Badge variant="outline">手动</Badge>
                  ) : (
                    <Badge variant="secondary">AI</Badge>
                  )}
                  {jargon.is_global && (
                    <Badge variant="outline" className="border-blue-500 text-blue-500">
                      全局
                    </Badge>
                  )}
                  {jargon.is_complete && (
                    <Badge variant="outline" className="border-purple-500 text-purple-500">
                      推断完成
                    </Badge>
                  )}
                </div>
              </div>
            </div>
          </div>
        </DialogBody>

        <DialogFooter className="flex-shrink-0">
          <Button onClick={() => onOpenChange(false)}>关闭</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ====================
// 黑话创建对话框
// ====================
interface JargonCreateDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  chatList: JargonChatInfo[]
  onSuccess: () => void
}

export function JargonCreateDialog({
  open,
  onOpenChange,
  chatList,
  onSuccess,
}: JargonCreateDialogProps) {
  const [formData, setFormData] = useState<JargonCreateRequest>({
    content: '',
    meaning: '',
    session_ids: [],
    is_global: false,
  })
  const [saving, setSaving] = useState(false)
  const { toast } = useToast()

  const handleCreate = async () => {
    if (!formData.content || !formData.session_ids?.length) {
      toast({
        title: '验证失败',
        description: '请填写必填字段：内容和聊天',
        variant: 'destructive',
      })
      return
    }

    try {
      setSaving(true)
      await createJargon(formData)
      toast({
        title: '创建成功',
        description: '黑话已创建',
      })
      setFormData({ content: '', meaning: '', session_ids: [], is_global: false })
      onSuccess()
    } catch (error) {
      toast({
        title: '创建失败',
        description: error instanceof Error ? error.message : '无法创建黑话',
        variant: 'destructive',
      })
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl" confirmOnEnter>
        <DialogHeader>
          <DialogTitle>新增黑话</DialogTitle>
          <DialogDescription>创建新的黑话记录</DialogDescription>
        </DialogHeader>

        <DialogBody>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="content">
                内容 <span className="text-destructive">*</span>
              </Label>
              <Input
                id="content"
                value={formData.content}
                onChange={(e) => setFormData({ ...formData, content: e.target.value })}
                placeholder="输入黑话内容"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="meaning">含义</Label>
              <Textarea
                id="meaning"
                value={formData.meaning || ''}
                onChange={(e) => setFormData({ ...formData, meaning: e.target.value })}
                placeholder="输入黑话含义（可选）"
                rows={3}
              />
            </div>

            <div className="space-y-2">
              <Label>
                聊天 <span className="text-destructive">*</span>
              </Label>
              <MultiSelect
                options={chatList.map((chat) => ({
                  label: chat.chat_name,
                  value: chat.session_id,
                }))}
                selected={formData.session_ids || []}
                onChange={(values) =>
                  setFormData({ ...formData, session_ids: values, session_id: values[0] })
                }
                placeholder="选择关联的聊天"
                emptyText="没有可选聊天"
              />
            </div>

            <div className="flex items-center space-x-2">
              <Switch
                id="is_global"
                checked={formData.is_global}
                onCheckedChange={(checked) => setFormData({ ...formData, is_global: checked })}
              />
              <Label htmlFor="is_global">设为全局黑话</Label>
            </div>
          </div>
        </DialogBody>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button data-dialog-action="confirm" onClick={handleCreate} disabled={saving}>
            {saving ? '创建中...' : '创建'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ====================
// 黑话编辑对话框
// ====================
interface JargonEditDialogProps {
  jargon: Jargon | null
  open: boolean
  onOpenChange: (open: boolean) => void
  chatList: JargonChatInfo[]
  onSuccess: () => void
}

export function JargonEditDialog({
  jargon,
  open,
  onOpenChange,
  chatList,
  onSuccess,
}: JargonEditDialogProps) {
  const [formData, setFormData] = useState<JargonUpdateRequest>({})
  const [saving, setSaving] = useState(false)
  const { toast } = useToast()

  useEffect(() => {
    if (jargon) {
      setFormData({
        content: jargon.content,
        meaning: jargon.meaning || '',
        session_id: jargon.session_id,
        session_ids: jargon.session_ids?.length
          ? jargon.session_ids
          : [jargon.session_id].filter(Boolean),
        is_global: jargon.is_global,
        is_jargon: jargon.is_jargon,
      })
    }
  }, [jargon])

  const handleSave = async () => {
    if (!jargon) return
    if (formData.session_ids && formData.session_ids.length === 0) {
      toast({
        title: '验证失败',
        description: '请至少选择一个聊天',
        variant: 'destructive',
      })
      return
    }

    try {
      setSaving(true)
      await updateJargon(jargon.id, formData)
      toast({
        title: '保存成功',
        description: '黑话已更新',
      })
      onSuccess()
    } catch (error) {
      toast({
        title: '保存失败',
        description: error instanceof Error ? error.message : '无法更新黑话',
        variant: 'destructive',
      })
    } finally {
      setSaving(false)
    }
  }

  if (!jargon) return null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl" confirmOnEnter>
        <DialogHeader>
          <DialogTitle>编辑黑话</DialogTitle>
          <DialogDescription>修改黑话的信息</DialogDescription>
        </DialogHeader>

        <DialogBody>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="edit_content">内容</Label>
              <Input
                id="edit_content"
                value={formData.content || ''}
                onChange={(e) => setFormData({ ...formData, content: e.target.value })}
                placeholder="输入黑话内容"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="edit_meaning">含义</Label>
              <Textarea
                id="edit_meaning"
                value={formData.meaning || ''}
                onChange={(e) => setFormData({ ...formData, meaning: e.target.value })}
                placeholder="输入黑话含义"
                rows={3}
              />
            </div>

            <div className="space-y-2">
              <Label>聊天</Label>
              <MultiSelect
                options={chatList.map((chat) => ({
                  label: chat.chat_name,
                  value: chat.session_id,
                }))}
                selected={formData.session_ids || []}
                onChange={(values) =>
                  setFormData({ ...formData, session_ids: values, session_id: values[0] })
                }
                placeholder="选择关联的聊天"
                emptyText="没有可选聊天"
              />
            </div>

            <div className="space-y-2">
              <Label>黑话状态</Label>
              <Select
                value={formData.is_jargon ? 'true' : 'false'}
                onValueChange={(value) =>
                  setFormData({
                    ...formData,
                    is_jargon: value === 'true',
                  })
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="true">是黑话</SelectItem>
                  <SelectItem value="false">无黑话</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center space-x-2">
              <Switch
                id="edit_is_global"
                checked={formData.is_global}
                onCheckedChange={(checked) => setFormData({ ...formData, is_global: checked })}
              />
              <Label htmlFor="edit_is_global">全局黑话</Label>
            </div>
          </div>
        </DialogBody>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button data-dialog-action="confirm" onClick={handleSave} disabled={saving}>
            {saving ? '保存中...' : '保存'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ====================
// 删除确认对话框
// ====================
interface DeleteConfirmDialogProps {
  jargon: Jargon | null
  open: boolean
  onOpenChange: () => void
  onConfirm: () => void
}

export function DeleteConfirmDialog({
  jargon,
  open,
  onOpenChange,
  onConfirm,
}: DeleteConfirmDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>确认删除</AlertDialogTitle>
          <AlertDialogDescription>
            确定要删除黑话 "{jargon?.content}" 吗？此操作不可撤销。
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>取消</AlertDialogCancel>
          <AlertDialogAction
            onClick={onConfirm}
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
          >
            删除
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}

// ====================
// 批量删除确认对话框
// ====================
interface BatchDeleteConfirmDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
  count: number
}

export function BatchDeleteConfirmDialog({
  open,
  onOpenChange,
  onConfirm,
  count,
}: BatchDeleteConfirmDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>确认批量删除</AlertDialogTitle>
          <AlertDialogDescription>
            您即将删除 {count} 个黑话，此操作无法撤销。确定要继续吗？
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>取消</AlertDialogCancel>
          <AlertDialogAction
            onClick={onConfirm}
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
          >
            确认删除
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
