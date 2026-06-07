import { useBlocker } from '@tanstack/react-router'
import { useState, useEffect, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { DraftNumberInput } from '@/components/ui/draft-number-input'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Slider } from '@/components/ui/slider'
import { Textarea } from '@/components/ui/textarea'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { ListFieldEditor } from '@/components/ListFieldEditor'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { CodeEditor } from '@/components/CodeEditor'
import { parse as parseToml } from 'smol-toml'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Settings,
  Package,
  ArrowUp,
  RefreshCw,
  ChevronRight,
  ChevronDown,
  Save,
  RotateCcw,
  Power,
  Loader2,
  Search,
  ArrowLeft,
  Info,
  Eye,
  EyeOff,
  RotateCw,
  Code2,
  Layout,
  Trash2,
} from 'lucide-react'
import { useToast } from '@/hooks/use-toast'
import { RestartProvider, useRestart } from '@/lib/restart-context'
import { RestartOverlay } from '@/components/restart-overlay'
import {
  fetchPluginList,
  getInstalledPlugins,
  getPluginConfigSchema,
  getPluginConfig,
  getPluginConfigRaw,
  updatePluginConfig,
  updatePluginConfigRaw,
  resetPluginConfig,
  togglePlugin,
  uninstallPlugin,
  updatePlugin,
  type InstalledPlugin,
  type PluginConfigSchema,
  type ConfigFieldSchema,
  type ConfigSectionSchema,
  type ItemFieldDefinition,
} from '@/lib/plugin-api'
import type { PluginInfo } from '@/types/plugin'
import { PluginIcon } from './plugins/PluginIcon'
import { getPluginTypeLabel } from './plugins/types'

// 字段渲染组件
interface FieldRendererProps {
  field: ConfigFieldSchema
  value: unknown
  onChange: (value: unknown) => void
  sectionName: string
}

function getLocaleCandidates(language: string): string[] {
  const normalized = (language || 'zh').replace('-', '_')
  const base = normalized.split('_')[0]
  const candidates = [language, normalized, base]

  if (base === 'zh') candidates.push('zh_CN', 'zh-CN')
  if (base === 'en') candidates.push('en_US', 'en-US')
  if (base === 'ja') candidates.push('ja_JP', 'ja-JP')
  if (base === 'ko') candidates.push('ko_KR', 'ko-KR')

  candidates.push('zh_CN', 'zh-CN', 'zh')
  return Array.from(new Set(candidates.filter(Boolean)))
}

function resolveLocalizedText(
  value: unknown,
  language: string,
  fallback = '',
  i18n?: Record<string, Record<string, string>>,
  key?: string,
): string {
  const candidates = getLocaleCandidates(language)

  if (i18n && key) {
    for (const locale of candidates) {
      const localized = i18n[locale]?.[key]
      if (typeof localized === 'string' && localized.trim()) {
        return localized
      }
    }
  }

  if (typeof value === 'string') {
    return value || fallback
  }

  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const localizedMap = value as Record<string, unknown>
    for (const locale of candidates) {
      const localized = localizedMap[locale]
      if (typeof localized === 'string' && localized.trim()) {
        return localized
      }
    }
  }

  return fallback
}

function localizeItemFields(
  itemFields: Record<string, ItemFieldDefinition> | undefined,
  language: string,
): Record<string, ItemFieldDefinition> | undefined {
  if (!itemFields) return undefined

  return Object.fromEntries(
    Object.entries(itemFields).map(([fieldName, field]) => [
      fieldName,
      {
        ...field,
        label: resolveLocalizedText(field.label, language, fieldName, field.i18n, 'label'),
        placeholder: resolveLocalizedText(field.placeholder, language, '', field.i18n, 'placeholder') || undefined,
      },
    ])
  )
}

function getNestedRecord(config: Record<string, unknown>, path?: string): Record<string, unknown> | undefined {
  if (!path) {
    return undefined
  }
  const parts = path.split('.').filter(Boolean)
  let current: unknown = config

  for (const part of parts) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) {
      return undefined
    }
    current = (current as Record<string, unknown>)[part]
  }

  if (!current || typeof current !== 'object' || Array.isArray(current)) {
    return undefined
  }

  return current as Record<string, unknown>
}

function setNestedField(
  config: Record<string, unknown>,
  path: string,
  fieldName: string,
  value: unknown,
): Record<string, unknown> {
  const parts = path.split('.').filter(Boolean)
  const nextConfig: Record<string, unknown> = { ...config }
  let currentTarget = nextConfig
  let currentSource: Record<string, unknown> | undefined = config

  for (const part of parts) {
    const sourceValue: unknown = currentSource?.[part]
    const nextValue =
      sourceValue && typeof sourceValue === 'object' && !Array.isArray(sourceValue)
        ? { ...(sourceValue as Record<string, unknown>) }
        : {}
    currentTarget[part] = nextValue
    currentTarget = nextValue
    currentSource =
      sourceValue && typeof sourceValue === 'object' && !Array.isArray(sourceValue)
        ? (sourceValue as Record<string, unknown>)
        : undefined
  }

  currentTarget[fieldName] = value
  return nextConfig
}

function FieldRenderer({ field, value, onChange }: FieldRendererProps) {
  const [showPassword, setShowPassword] = useState(false)
  const { i18n } = useTranslation()
  const language = i18n.resolvedLanguage || i18n.language || 'zh'
  const label = resolveLocalizedText(field.label, language, field.name, field.i18n, 'label')
  const hint = resolveLocalizedText(field.hint, language, '', field.i18n, 'hint')
  const placeholder = resolveLocalizedText(field.placeholder, language, '', field.i18n, 'placeholder')
  const localizedItemFields = localizeItemFields(field.item_fields, language)

  // 根据 ui_type 渲染不同的控件
  switch (field.ui_type) {
    case 'switch':
      return (
        <div className="flex items-center justify-between">
          <div className="space-y-0.5">
            <Label>{label}</Label>
            {hint && (
              <p className="text-xs text-muted-foreground">{hint}</p>
            )}
          </div>
          <Switch
            checked={Boolean(value ?? field.default)}
            onCheckedChange={onChange}
            disabled={field.disabled}
          />
        </div>
      )

    case 'number':
      return (
        <div className="space-y-2">
          <Label>{label}</Label>
          <DraftNumberInput
            value={value}
            defaultValue={field.default}
            onValueChange={onChange}
            min={field.min}
            max={field.max}
            step={field.step ?? 1}
            placeholder={placeholder}
            disabled={field.disabled}
          />
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )

    case 'slider':
      return (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label>{label}</Label>
            <span className="text-sm text-muted-foreground">
              {value as number ?? field.default}
            </span>
          </div>
          <Slider
            value={[value as number ?? field.default as number]}
            onValueChange={(v) => onChange(v[0])}
            min={field.min ?? 0}
            max={field.max ?? 100}
            step={field.step ?? 1}
            disabled={field.disabled}
          />
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )

    case 'select':
      return (
        <div className="space-y-2">
          <Label>{label}</Label>
          <Select
            value={String(value ?? field.default)}
            onValueChange={onChange}
            disabled={field.disabled}
          >
            <SelectTrigger>
              <SelectValue placeholder={placeholder || '请选择'} />
            </SelectTrigger>
            <SelectContent>
              {field.choices?.map((choice) => (
                <SelectItem key={String(choice)} value={String(choice)}>
                  {String(choice)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )

    case 'textarea':
      return (
        <div className="space-y-2">
          <Label>{label}</Label>
          <Textarea
            value={value as string ?? field.default}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            rows={field.rows ?? 3}
            disabled={field.disabled}
          />
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )

    case 'password':
      return (
        <div className="space-y-2">
          <Label>{label}</Label>
          <div className="relative">
            <Input
              type={showPassword ? 'text' : 'password'}
              value={value as string ?? ''}
              onChange={(e) => onChange(e.target.value)}
              placeholder={placeholder}
              disabled={field.disabled}
              className="pr-10"
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="absolute right-0 top-0 h-full px-3"
              onClick={() => setShowPassword(!showPassword)}
            >
              {showPassword ? (
                <EyeOff className="h-4 w-4" />
              ) : (
                <Eye className="h-4 w-4" />
              )}
            </Button>
          </div>
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )

    case 'list':
      return (
        <div className="space-y-2">
          <Label>{label}</Label>
          <ListFieldEditor
            value={Array.isArray(value) ? value : (Array.isArray(field.default) ? field.default : [])}
            onChange={(newValue) => onChange(newValue)}
            itemType={field.item_type ?? 'string'}
            itemFields={localizedItemFields}
            minItems={field.min_items}
            maxItems={field.max_items}
            disabled={field.disabled}
            placeholder={placeholder}
          />
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )

    case 'text':
    default:
      return (
        <div className="space-y-2">
          <Label>{label}</Label>
          <Input
            type="text"
            value={value as string ?? field.default ?? ''}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            maxLength={field.max_length}
            disabled={field.disabled}
          />
          {hint && (
            <p className="text-xs text-muted-foreground">{hint}</p>
          )}
        </div>
      )
  }
}

// Section 渲染组件
interface SectionRendererProps {
  sectionName: string
  section: ConfigSectionSchema
  config: Record<string, unknown>
  onChange: (sectionName: string, fieldName: string, value: unknown) => void
}

function SectionRenderer({ sectionName, section, config, onChange }: SectionRendererProps) {
  const [isOpen, setIsOpen] = useState(!section.collapsed)
  const { i18n } = useTranslation()
  const language = i18n.resolvedLanguage || i18n.language || 'zh'
  const resolvedSectionName = section.name || sectionName
  const sectionConfig = getNestedRecord(config, resolvedSectionName)
  const title = resolveLocalizedText(section.title, language, sectionName, section.i18n, 'title')
  const description = resolveLocalizedText(section.description, language, '', section.i18n, 'description')
  
  // 按 order 排序字段
  const sortedFields = Object.entries(section.fields)
    .filter(([, field]) => !field.hidden)
    .sort(([, a], [, b]) => (a.order ?? 0) - (b.order ?? 0))

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen}>
      <Card>
        <CollapsibleTrigger asChild>
          <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                {isOpen ? (
                  <ChevronDown className="h-4 w-4 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 text-muted-foreground" />
                )}
                <CardTitle className="text-lg">{title}</CardTitle>
              </div>
              <Badge variant="secondary" className="text-xs">
                {sortedFields.length} 项
              </Badge>
            </div>
            {description && (
              <CardDescription className="ml-6">
                {description}
              </CardDescription>
            )}
          </CardHeader>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <CardContent className="space-y-4 pt-0">
            {sortedFields.map(([fieldName, field]) => (
              <FieldRenderer
                key={fieldName}
                field={field}
                value={sectionConfig?.[fieldName]}
                onChange={(value) => onChange(resolvedSectionName, fieldName, value)}
                sectionName={resolvedSectionName}
              />
            ))}
          </CardContent>
        </CollapsibleContent>
      </Card>
    </Collapsible>
  )
}

// 插件配置编辑器
interface PluginConfigEditorProps {
  plugin: InstalledPlugin
  onBack: () => void
  initialTab?: string
}

function PluginConfigEditor({ plugin, onBack, initialTab }: PluginConfigEditorProps) {
  const { toast } = useToast()
  const { triggerRestart, isRestarting } = useRestart()
  const { i18n } = useTranslation()
  const language = i18n.resolvedLanguage || i18n.language || 'zh'
  const [editMode, setEditMode] = useState<'visual' | 'source'>('visual')
  const [schema, setSchema] = useState<PluginConfigSchema | null>(null)
  const [activeConfigTab, setActiveConfigTab] = useState<string | undefined>(initialTab)
  const [config, setConfig] = useState<Record<string, unknown>>({})
  const [originalConfig, setOriginalConfig] = useState<Record<string, unknown>>({})
  const [sourceCode, setSourceCode] = useState('')
  const [originalSourceCode, setOriginalSourceCode] = useState('')
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [hasChanges, setHasChanges] = useState(false)
  const [hasTomlError, setHasTomlError] = useState(false)
  const [resetDialogOpen, setResetDialogOpen] = useState(false)
  const [internalLeavePromptOpen, setInternalLeavePromptOpen] = useState(false)

  const navigationBlocker = useBlocker({
    shouldBlockFn: () => hasChanges,
    enableBeforeUnload: hasChanges,
    withResolver: true,
  })

  // 加载配置
  const loadConfig = useCallback(async () => {
    setLoading(true)
    try {
      const [schemaResult, configResult, rawResult] = await Promise.all([
        getPluginConfigSchema(plugin.id),
        getPluginConfig(plugin.id),
        getPluginConfigRaw(plugin.id)
      ])
      
      if (!schemaResult.success) {
        toast({
          title: '加载配置架构失败',
          description: schemaResult.error,
          variant: 'destructive'
        })
        return
      }
      
      if (!configResult.success) {
        toast({
          title: '加载配置数据失败',
          description: configResult.error,
          variant: 'destructive'
        })
        return
      }
      
      if (!rawResult.success) {
        toast({
          title: '加载原始配置失败',
          description: rawResult.error,
          variant: 'destructive'
        })
        return
      }
      
      setSchema(schemaResult.data)
      setConfig(configResult.data)
      setOriginalConfig(JSON.parse(JSON.stringify(configResult.data)))
      setSourceCode(rawResult.data)
      setOriginalSourceCode(rawResult.data)
    } catch (error) {
      toast({
        title: '加载配置失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    } finally {
      setLoading(false)
    }
  }, [plugin.id, toast])

  useEffect(() => {
    loadConfig()
  }, [loadConfig])

  // 检测配置变化
  useEffect(() => {
    if (editMode === 'visual') {
      setHasChanges(JSON.stringify(config) !== JSON.stringify(originalConfig))
    } else {
      setHasChanges(sourceCode !== originalSourceCode)
    }
  }, [config, originalConfig, sourceCode, originalSourceCode, editMode])

  // 处理字段变化
  const handleFieldChange = (sectionName: string, fieldName: string, value: unknown) => {
    setConfig(prev => setNestedField(prev, sectionName, fieldName, value))
  }

  // 保存配置
  const handleSave = async (): Promise<boolean> => {
    setSaving(true)
    try {
      if (editMode === 'source') {
        // 源代码模式：先验证 TOML 格式
        try {
          parseToml(sourceCode)
        } catch (error) {
          setHasTomlError(true)
          toast({
            title: 'TOML 格式错误',
            description: error instanceof Error ? error.message : '无法解析 TOML 配置，请检查语法',
            variant: 'destructive'
          })
          setSaving(false)
          return false
        }
        
        // 格式正确，保存原始配置
        await updatePluginConfigRaw(plugin.id, sourceCode)
        setOriginalSourceCode(sourceCode)
        setHasTomlError(false)
      } else {
        // 可视化模式
        await updatePluginConfig(plugin.id, config)
        setOriginalConfig(JSON.parse(JSON.stringify(config)))
      }
      
      toast({
        title: '配置已保存',
        description: '更改将在插件重新加载后生效'
      })
      return true
    } catch (error) {
      toast({
        title: '保存失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
      return false
    } finally {
      setSaving(false)
    }
  }

  const handleBack = () => {
    if (!hasChanges) {
      onBack()
      return
    }
    setInternalLeavePromptOpen(true)
  }

  const closeLeavePrompt = () => {
    if (navigationBlocker.status === 'blocked') {
      navigationBlocker.reset?.()
    }
    setInternalLeavePromptOpen(false)
  }

  const leaveWithoutSaving = () => {
    if (internalLeavePromptOpen) {
      setInternalLeavePromptOpen(false)
      onBack()
      return
    }
    navigationBlocker.proceed?.()
  }

  const saveAndLeave = async () => {
    const saved = await handleSave()
    if (!saved) {
      return
    }
    if (internalLeavePromptOpen) {
      setInternalLeavePromptOpen(false)
      onBack()
      return
    }
    navigationBlocker.proceed?.()
  }

  // 重置配置
  const handleReset = async () => {
    try {
      const resetResult = await resetPluginConfig(plugin.id)
      if (!resetResult.success) {
        toast({
          title: '重置失败',
          description: resetResult.error,
          variant: 'destructive'
        })
        return
      }
      toast({
        title: '配置已重置',
        description: '下次加载插件时将使用默认配置'
      })
      setResetDialogOpen(false)
      loadConfig()
    } catch (error) {
      toast({
        title: '重置失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    }
  }

  // 切换启用状态
  const handleToggle = async () => {
    try {
      const toggleResult = await togglePlugin(plugin.id)
      if (!toggleResult.success) {
        toast({
          title: '切换失败',
          description: toggleResult.error,
          variant: 'destructive'
        })
        return
      }
      toast({
        title: toggleResult.data.message,
        description: toggleResult.data.note
      })
      loadConfig()
    } catch (error) {
      toast({
        title: '切换状态失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (!schema) {
    return (
      <div className="flex flex-col items-center justify-center h-64 space-y-4">
        <AlertCircle className="h-12 w-12 text-muted-foreground" />
        <p className="text-muted-foreground">无法加载配置</p>
        <Button onClick={onBack} variant="outline">
          <ArrowLeft className="h-4 w-4 mr-2" />
          返回
        </Button>
      </div>
    )
  }

  // 按 order 排序 sections
  const sortedSections = Object.entries(schema.sections)
    .sort(([, a], [, b]) => (a.order ?? 0) - (b.order ?? 0))
  const schemaTabs = schema.layout.type === 'tabs' ? schema.layout.tabs : []
  const selectedConfigTab = schemaTabs.some((tab) => tab.id === activeConfigTab)
    ? activeConfigTab
    : schemaTabs[0]?.id

  const handleConfigTabChange = (nextTab: string) => {
    setActiveConfigTab(nextTab)
    const params = new URLSearchParams({ plugin: plugin.id, tab: nextTab })
    window.history.replaceState(null, '', `/plugin-config?${params.toString()}`)
  }

  // 获取当前启用状态
  const isEnabled = (config.plugin as Record<string, unknown>)?.enabled !== false
  const pluginName = resolveLocalizedText(
    schema.plugin_info.name,
    language,
    plugin.manifest.name,
    schema.plugin_info.i18n,
    'name',
  )

  return (
    <div className="space-y-4 sm:space-y-6">
      {/* 头部 */}
      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-4">
        <div className="flex items-start gap-3">
          <Button variant="ghost" size="icon" onClick={handleBack}>
            <ArrowLeft className="h-5 w-5" />
          </Button>
          <div>
            <h1 className="text-2xl sm:text-3xl font-bold" data-plugin-config-title>
              {pluginName}
            </h1>
            <div className="flex items-center gap-2 mt-1">
              <Badge variant={isEnabled ? 'default' : 'secondary'}>
                {isEnabled ? '已启用' : '已禁用'}
              </Badge>
              <span className="text-sm text-muted-foreground">
                v{schema.plugin_info.version || plugin.manifest.version}
              </span>
            </div>
          </div>
        </div>
        <div className="ml-10 flex flex-wrap gap-3 sm:ml-0">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setEditMode(editMode === 'visual' ? 'source' : 'visual')}
          >
            {editMode === 'visual' ? (
              <>
                <Code2 className="h-4 w-4 mr-2" />
                源代码
              </>
            ) : (
              <>
                <Layout className="h-4 w-4 mr-2" />
                可视化
              </>
            )}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => triggerRestart()}
            disabled={isRestarting}
          >
            <RotateCw className={`h-4 w-4 mr-2 ${isRestarting ? 'animate-spin' : ''}`} />
            重启麦麦
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleToggle}
          >
            <Power className="h-4 w-4 mr-2" />
            {isEnabled ? '禁用' : '启用'}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setResetDialogOpen(true)}
          >
            <RotateCcw className="h-4 w-4 mr-2" />
            重置
          </Button>
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!hasChanges || saving}
          >
            {saving ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Save className="h-4 w-4 mr-2" />
            )}
            保存
          </Button>
        </div>
      </div>

      {/* 未保存提示 */}
      {hasChanges && (
        <Card className="border-orange-200 bg-orange-50 dark:bg-orange-950/20 dark:border-orange-900">
          <CardContent className="py-3">
            <div className="flex items-center gap-2">
              <Info className="h-4 w-4 text-orange-600" />
              <p className="text-sm text-orange-800 dark:text-orange-200">
                有未保存的更改
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {/* 源代码模式 */}
      {editMode === 'source' && (
        <div className="space-y-4">
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>
              <strong>源代码模式（高级功能）：</strong>直接编辑 TOML 配置文件。保存时会验证格式，只有格式正确才能保存。
              {hasTomlError && (
                <span className="text-destructive font-semibold ml-2">⚠️ 上次保存失败，请检查 TOML 格式</span>
              )}
            </AlertDescription>
          </Alert>
          
            <CodeEditor
              value={sourceCode}
              onChange={(value) => {
                setSourceCode(value)
                if (hasTomlError) {
                  setHasTomlError(false)
                }
              }}
              language="toml"
              height="calc(100vh - 350px)"
              minHeight="500px"
              placeholder="TOML 配置内容"
            />
        </div>
      )}

      {/* 可视化模式 */}
      {editMode === 'visual' && (
      <>
      {/* 配置区域 */}
      {schema.layout.type === 'tabs' && schemaTabs.length > 0 ? (
        // 标签页布局
        <Tabs value={selectedConfigTab} onValueChange={handleConfigTabChange}>
          <TabsList>
            {schemaTabs.map(tab => (
              <TabsTrigger key={tab.id} value={tab.id}>
                {resolveLocalizedText(tab.title, language, tab.id, tab.i18n, 'title')}
                {tab.badge && (
                  <Badge variant="secondary" className="ml-2 text-xs">
                    {tab.badge}
                  </Badge>
                )}
              </TabsTrigger>
            ))}
          </TabsList>
          {schemaTabs.map(tab => (
            <TabsContent key={tab.id} value={tab.id} className="space-y-4 mt-4">
              {tab.sections.map(sectionName => {
                const section = schema.sections[sectionName]
                if (!section) return null
                return (
                  <SectionRenderer
                    key={sectionName}
                    sectionName={sectionName}
                    section={section}
                    config={config}
                    onChange={handleFieldChange}
                  />
                )
              })}
            </TabsContent>
          ))}
        </Tabs>
      ) : (
        // 自动布局
        <div className="space-y-4">
          {sortedSections.map(([sectionName, section]) => (
            <SectionRenderer
              key={sectionName}
              sectionName={sectionName}
              section={section}
              config={config}
              onChange={handleFieldChange}
            />
          ))}
        </div>
      )}
      </>
      )}

      <Dialog
        open={internalLeavePromptOpen || navigationBlocker.status === 'blocked'}
        onOpenChange={(open) => {
          if (!open) {
            closeLeavePrompt()
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>有未保存的更改</DialogTitle>
            <DialogDescription>
              当前插件配置文件有修改，离开页面前是否保存？
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={closeLeavePrompt} disabled={saving}>
              取消
            </Button>
            <Button variant="outline" onClick={leaveWithoutSaving} disabled={saving}>
              不保存
            </Button>
            <Button onClick={saveAndLeave} disabled={saving}>
              {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Save className="h-4 w-4 mr-2" />}
              保存并离开
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 重置确认对话框 */}
      <Dialog open={resetDialogOpen} onOpenChange={setResetDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>确认重置配置</DialogTitle>
            <DialogDescription>
              这将删除当前配置文件，下次加载插件时将使用默认配置。此操作不可撤销。
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setResetDialogOpen(false)}>
              取消
            </Button>
            <Button variant="destructive" onClick={handleReset}>
              确认重置
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

// 主页面组件 - 包装 RestartProvider
function getInitialPluginConfigTarget(): { pluginId: string | null; tabId: string | null } {
  if (typeof window === 'undefined') {
    return { pluginId: null, tabId: null }
  }

  const params = new URLSearchParams(window.location.search)
  return {
    pluginId: params.get('plugin'),
    tabId: params.get('tab'),
  }
}

function comparePluginVersions(currentVersion: string, latestVersion: string): number {
  const currentParts = currentVersion.trim().split('.').map(part => Number.parseInt(part, 10) || 0)
  const latestParts = latestVersion.trim().split('.').map(part => Number.parseInt(part, 10) || 0)
  const maxLength = Math.max(currentParts.length, latestParts.length)

  for (let index = 0; index < maxLength; index++) {
    const currentPart = currentParts[index] || 0
    const latestPart = latestParts[index] || 0
    if (latestPart > currentPart) return 1
    if (latestPart < currentPart) return -1
  }

  return 0
}

export function PluginConfigPage() {
  return (
    <RestartProvider>
      <PluginConfigPageContent />
    </RestartProvider>
  )
}

// 内部组件：实际内容
function PluginConfigPageContent() {
  const { toast } = useToast()
  const initialTarget = getInitialPluginConfigTarget()
  const [plugins, setPlugins] = useState<InstalledPlugin[]>([])
  const [loading, setLoading] = useState(true)
  const [searchQuery, setSearchQuery] = useState('')
  const [showUpdateOnly, setShowUpdateOnly] = useState(false)
  const [selectedPlugin, setSelectedPlugin] = useState<InstalledPlugin | null>(null)
  const [selectedPluginTab, setSelectedPluginTab] = useState<string | undefined>(initialTarget.tabId ?? undefined)
  const [actingPluginId, setActingPluginId] = useState<string | null>(null)
  const [marketPluginsById, setMarketPluginsById] = useState<Record<string, PluginInfo>>({})
  const [checkingUpdates, setCheckingUpdates] = useState(false)

  const openPluginConfig = (plugin: InstalledPlugin, tabId?: string | null) => {
    setSelectedPlugin(plugin)
    setSelectedPluginTab(tabId ?? undefined)
    const params = new URLSearchParams({ plugin: plugin.id })
    if (tabId) {
      params.set('tab', tabId)
    }
    window.history.replaceState(null, '', `/plugin-config?${params.toString()}`)
  }

  const closePluginConfig = () => {
    setSelectedPlugin(null)
    setSelectedPluginTab(undefined)
    window.history.replaceState(null, '', '/plugin-config')
  }

  // 加载插件列表
  const loadPlugins = async () => {
    setLoading(true)
    try {
      const installedResult = await getInstalledPlugins()
      if (!installedResult.success) {
        toast({
          title: '加载插件列表失败',
          description: installedResult.error,
          variant: 'destructive'
        })
        return
      }
      setPlugins(installedResult.data)
      if (!selectedPlugin && initialTarget.pluginId) {
        const targetPlugin = installedResult.data.find((plugin) => plugin.id === initialTarget.pluginId)
        if (targetPlugin) {
          openPluginConfig(targetPlugin, initialTarget.tabId)
        }
      }
    } catch (error) {
      toast({
        title: '加载插件列表失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    } finally {
      setLoading(false)
    }
  }

  const checkPluginUpdates = async () => {
    setCheckingUpdates(true)
    try {
      const marketResult = await fetchPluginList()
      if (!marketResult.success) {
        console.warn('加载插件市场版本信息失败:', marketResult.error)
        setMarketPluginsById({})
        return
      }

      const nextMarketPluginsById: Record<string, PluginInfo> = {}
      for (const marketPlugin of marketResult.data) {
        nextMarketPluginsById[marketPlugin.id] = marketPlugin
        if (marketPlugin.manifest.id) {
          nextMarketPluginsById[marketPlugin.manifest.id] = marketPlugin
        }
      }
      setMarketPluginsById(nextMarketPluginsById)
    } catch (error) {
      console.warn('加载插件市场版本信息失败:', error)
      setMarketPluginsById({})
    } finally {
      setCheckingUpdates(false)
    }
  }

  useEffect(() => {
    loadPlugins()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (!loading) {
      void checkPluginUpdates()
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loading])

  // 过滤插件
  const filteredPlugins = plugins.filter(plugin => {
    const query = searchQuery.toLowerCase()
    return (
      plugin.id.toLowerCase().includes(query) ||
      plugin.manifest.name.toLowerCase().includes(query) ||
      plugin.manifest.description?.toLowerCase().includes(query)
    )
  })
  
  // 去重：如果有重复的 plugin.id，只保留第一个
  const uniqueFilteredPlugins = filteredPlugins.filter((plugin, index, self) =>
    index === self.findIndex((p) => p.id === plugin.id)
  )

  // 统计数据
  const isPluginDisabled = (plugin: InstalledPlugin) => plugin.disabled === true || plugin.enabled === false
  const isPluginLoadSuccess = (plugin: InstalledPlugin) => !isPluginDisabled(plugin) && (
    plugin.load_status === 'success' || plugin.loaded === true
  )
  const isPluginLoadFailed = (plugin: InstalledPlugin) => !isPluginDisabled(plugin) && !isPluginLoadSuccess(plugin)
  const installedCount = plugins.length
  const disabledCount = plugins.filter(isPluginDisabled).length
  const enabledCount = installedCount - disabledCount
  const loadSuccessCount = plugins.filter(isPluginLoadSuccess).length
  const loadFailedCount = plugins.filter(isPluginLoadFailed).length
  const loadTotalCount = loadSuccessCount + loadFailedCount
  const loadSuccessPercent = loadTotalCount > 0 ? (loadSuccessCount / loadTotalCount) * 100 : 0
  const loadFailedPercent = loadTotalCount > 0 ? (loadFailedCount / loadTotalCount) * 100 : 0
  const getPluginStatusMeta = (plugin: InstalledPlugin) => {
    if (isPluginDisabled(plugin)) {
      return { dotClassName: 'bg-muted-foreground/45', label: '已禁用' }
    }
    if (isPluginLoadSuccess(plugin)) {
      return { dotClassName: 'bg-emerald-500 shadow-[0_0_0_3px_rgba(16,185,129,0.16)]', label: '加载成功' }
    }
    return { dotClassName: 'bg-red-500 shadow-[0_0_0_3px_rgba(239,68,68,0.16)]', label: '加载失败' }
  }
  const getPluginRepositoryUrl = (plugin: InstalledPlugin): string | undefined => {
    const marketPlugin = marketPluginsById[plugin.id] || (plugin.manifest.id ? marketPluginsById[plugin.manifest.id] : undefined)
    const urls = plugin.manifest.urls as { repository?: string } | undefined
    return plugin.manifest.repository_url || urls?.repository || marketPlugin?.manifest.repository_url || marketPlugin?.manifest.urls?.repository
  }
  const getPluginUpdateState = (plugin: InstalledPlugin): { canUpdate: boolean; hasUpdate: boolean; title?: string } => {
    if (checkingUpdates) {
      return { canUpdate: false, hasUpdate: false, title: '正在检查更新' }
    }

    const marketPlugin = marketPluginsById[plugin.id] || (plugin.manifest.id ? marketPluginsById[plugin.manifest.id] : undefined)
    if (!marketPlugin) {
      return { canUpdate: false, hasUpdate: false, title: '插件市场中没有找到该插件，无法判断新版本' }
    }

    if (!getPluginRepositoryUrl(plugin)) {
      return { canUpdate: false, hasUpdate: false, title: '插件清单中没有仓库地址，无法更新/升级' }
    }

    const currentVersion = plugin.manifest.version
    const latestVersion = marketPlugin.manifest.version
    if (comparePluginVersions(currentVersion, latestVersion) <= 0) {
      return { canUpdate: false, hasUpdate: false, title: '当前已是最新版本' }
    }

    return { canUpdate: true, hasUpdate: true, title: `发现新版本 v${latestVersion}` }
  }
  const visiblePlugins = showUpdateOnly
    ? uniqueFilteredPlugins.filter((plugin) => getPluginUpdateState(plugin).hasUpdate)
    : uniqueFilteredPlugins

  const stopPluginActionEvent = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault()
    event.stopPropagation()
  }

  const handleTogglePlugin = async (plugin: InstalledPlugin, event: React.MouseEvent<HTMLButtonElement>) => {
    stopPluginActionEvent(event)
    setActingPluginId(plugin.id)
    try {
      const toggleResult = await togglePlugin(plugin.id)
      if (!toggleResult.success) {
        toast({
          title: '切换插件状态失败',
          description: toggleResult.error,
          variant: 'destructive'
        })
        return
      }

      toast({
        title: toggleResult.data.enabled ? '插件已启动' : '插件已关闭',
        description: toggleResult.data.message || `${plugin.manifest.name} 状态已更新`
      })
      await loadPlugins()
    } catch (error) {
      toast({
        title: '切换插件状态失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    } finally {
      setActingPluginId(null)
    }
  }

  const handleUpdatePlugin = async (plugin: InstalledPlugin, event: React.MouseEvent<HTMLButtonElement>) => {
    stopPluginActionEvent(event)

    const repositoryUrl = getPluginRepositoryUrl(plugin)
    if (!repositoryUrl) {
      return
    }

    setActingPluginId(plugin.id)
    try {
      const updateResult = await updatePlugin(plugin.id, repositoryUrl, 'main')
      if (!updateResult.success) {
        toast({
          title: '更新插件失败',
          description: updateResult.error,
          variant: 'destructive'
        })
        return
      }

      toast({
        title: '更新插件成功',
        description: `${plugin.manifest.name} 已完成更新/升级`
      })
      await loadPlugins()
    } catch (error) {
      toast({
        title: '更新插件失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    } finally {
      setActingPluginId(null)
    }
  }

  const handleDeletePlugin = async (plugin: InstalledPlugin, event: React.MouseEvent<HTMLButtonElement>) => {
    stopPluginActionEvent(event)
    setActingPluginId(plugin.id)
    try {
      const uninstallResult = await uninstallPlugin(plugin.id)
      if (!uninstallResult.success) {
        toast({
          title: '删除插件失败',
          description: uninstallResult.error,
          variant: 'destructive'
        })
        return
      }

      toast({
        title: '删除插件成功',
        description: `${plugin.manifest.name} 已删除`
      })
      await loadPlugins()
    } catch (error) {
      toast({
        title: '删除插件失败',
        description: error instanceof Error ? error.message : '未知错误',
        variant: 'destructive'
      })
    } finally {
      setActingPluginId(null)
    }
  }

  // 如果选中了插件，显示配置编辑器
  if (selectedPlugin) {
    return (
      <>
        <ScrollArea className="h-full">
          <div className="p-4 sm:p-6">
            <PluginConfigEditor
              plugin={selectedPlugin}
              initialTab={selectedPluginTab}
              onBack={closePluginConfig}
            />
          </div>
        </ScrollArea>
        <RestartOverlay />
      </>
    )
  }

  return (
    <ScrollArea className="h-full">
      <div className="space-y-4 sm:space-y-6 p-4 sm:p-6">
        <div className="flex flex-wrap items-center gap-3">
          <div className="relative min-w-0 flex-1 basis-72">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="搜索插件..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
            />
          </div>
          <div className="flex h-9 items-center gap-2 rounded-md border px-3">
            <Label htmlFor="show-update-only" className="cursor-pointer text-sm font-medium">
              有更新
            </Label>
            <Switch
              id="show-update-only"
              checked={showUpdateOnly}
              disabled={checkingUpdates}
              onCheckedChange={setShowUpdateOnly}
            />
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={loadPlugins}
            aria-label="刷新"
            title="刷新"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </Button>
        </div>

        {/* 统计信息 */}
        <Card>
          <CardContent className="space-y-3 p-4">
            <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
              <span className="flex items-center gap-2">
                <Package className="h-4 w-4 text-muted-foreground" />
                已安装 <strong>{installedCount}</strong> 个插件
              </span>
              <span>已启用 <strong className="text-emerald-600">{enabledCount}</strong> 个</span>
              <span>已禁用 <strong className="text-muted-foreground">{disabledCount}</strong> 个</span>
            </div>
            <div
              className="flex items-center gap-3 border-t pt-3 text-sm"
              aria-label={`加载成功 ${loadSuccessCount} 个，加载失败 ${loadFailedCount} 个`}
            >
              <span className="sr-only">加载成功 {loadSuccessCount} 个，加载失败 {loadFailedCount} 个</span>
              <strong className="w-8 text-right text-emerald-600">{loadSuccessCount}</strong>
              <div className="flex h-3 min-w-28 flex-1 overflow-hidden bg-muted" aria-hidden="true">
                <div className="bg-emerald-500" style={{ width: `${loadSuccessPercent}%` }} />
                <div className="bg-red-500" style={{ width: `${loadFailedPercent}%` }} />
              </div>
              <strong className="w-8 text-red-600">{loadFailedCount}</strong>
            </div>
          </CardContent>
        </Card>

        {/* 插件列表 */}
        <Card>
          <CardContent>
            {loading ? (
              <div className="flex items-center justify-center py-12">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : visiblePlugins.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 space-y-4">
                <Package className="h-16 w-16 text-muted-foreground/50" />
                <div className="text-center space-y-2">
                  <p className="text-lg font-medium text-muted-foreground">
                    {showUpdateOnly ? '暂无可更新插件' : searchQuery ? '没有找到匹配的插件' : '暂无已安装的插件'}
                  </p>
                  <p className="text-sm text-muted-foreground">
                    {showUpdateOnly ? '当前已安装插件没有发现新版本' : searchQuery ? '尝试其他搜索关键词' : '前往插件市场安装插件'}
                  </p>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                {visiblePlugins.map(plugin => {
                  const statusMeta = getPluginStatusMeta(plugin)
                  const pluginActing = actingPluginId === plugin.id
                  const pluginDisabled = isPluginDisabled(plugin)
                  const updateState = getPluginUpdateState(plugin)
                  return (
                  <div
                    key={plugin.id}
                    className={`relative flex min-h-32 cursor-pointer flex-col justify-between gap-4 rounded-lg border p-5 transition-colors hover:bg-muted/50 sm:min-h-0 sm:flex-row sm:items-center sm:p-4 ${isPluginDisabled(plugin) ? 'opacity-70' : ''}`}
                    role="button"
                    tabIndex={0}
                    onClick={() => openPluginConfig(plugin)}
                    onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openPluginConfig(plugin) } }}
                  >
                    <div className="flex min-w-0 items-start gap-3 sm:items-center">
                      <span
                        className={`mt-4 h-2.5 w-2.5 flex-shrink-0 rounded-full sm:mt-0 ${statusMeta.dotClassName}`}
                        title={statusMeta.label}
                        aria-label={statusMeta.label}
                      />
                      <PluginIcon pluginId={plugin.id} manifest={plugin.manifest} installed className="h-12 w-12 sm:h-10 sm:w-10" />
                      <div className="min-w-0 flex-1 space-y-2 sm:space-y-1">
                        <div className="flex min-w-0 flex-wrap items-center gap-2">
                          <h3 className="min-w-0 break-words text-sm font-medium leading-snug sm:truncate sm:text-base">
                            {plugin.manifest.name}
                          </h3>
                          <Badge variant="secondary" className="text-xs flex-shrink-0">
                            v{plugin.manifest.version}
                          </Badge>
                          <Badge variant="outline" className="text-xs flex-shrink-0">
                            {getPluginTypeLabel(plugin)}
                          </Badge>
                        </div>
                        <p className="line-clamp-2 text-sm leading-relaxed text-muted-foreground sm:truncate sm:leading-normal">
                          {plugin.manifest.description || '暂无描述'}
                        </p>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-2 border-t pt-3 sm:flex sm:flex-shrink-0 sm:items-center sm:justify-end sm:border-t-0 sm:pt-0">
                      <Button variant="ghost" size="sm" className="min-w-24 sm:min-w-0">
                        <Settings className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={pluginActing}
                        onClick={(event) => handleTogglePlugin(plugin, event)}
                      >
                        {pluginActing ? (
                          <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                        ) : (
                          <Power className="mr-1 h-4 w-4" />
                        )}
                        {pluginDisabled ? '启动' : '关闭'}
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="relative h-9 w-9 p-0"
                        disabled={pluginActing || !updateState.canUpdate}
                        title={updateState.title}
                        aria-label={updateState.title || '更新/升级'}
                        onClick={(event) => handleUpdatePlugin(plugin, event)}
                      >
                        {updateState.hasUpdate && (
                          <span
                            className="absolute -right-1 -top-1 h-3 w-3 rounded-sm bg-yellow-400 ring-2 ring-background"
                            aria-hidden="true"
                          />
                        )}
                        {pluginActing ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : checkingUpdates ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <ArrowUp className="h-4 w-4" />
                        )}
                      </Button>
                      <Button
                        variant="destructive"
                        size="sm"
                        className="h-9 w-9 p-0"
                        disabled={pluginActing}
                        title="删除"
                        aria-label="删除"
                        onClick={(event) => handleDeletePlugin(plugin, event)}
                      >
                        {pluginActing ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Trash2 className="h-4 w-4" />
                        )}
                      </Button>
                      <ChevronRight className="h-4 w-4 text-muted-foreground" />
                    </div>
                  </div>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </ScrollArea>
  )
}
