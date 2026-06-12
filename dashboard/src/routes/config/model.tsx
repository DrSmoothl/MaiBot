import { useState, useEffect, useCallback, useRef, type MouseEvent } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { ThinkingIllustration } from '@/components/ui/thinking-illustration'
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { Switch } from '@/components/ui/switch'
import { Slider } from '@/components/ui/slider'
import { Badge } from '@/components/ui/badge'
import { Plus, Trash2, Save, Search, Info, Check, ChevronsUpDown, RefreshCw, Loader2, GraduationCap, Share2, AlertTriangle, Settings, Zap } from 'lucide-react'
import {
  getModelConfig,
  getModelConfigCached,
  getModelConfigSchema,
  testProviderConnection,
  updateModelConfig,
  updateModelConfigSection,
} from '@/lib/config-api'
import type { TestConnectionResult } from '@/lib/config-api'
import { resolveFieldLabel } from '@/lib/config-label'
import type { ConfigSchema } from '@/types/config-schema'
import { useToast } from '@/hooks/use-toast'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { HelpTooltip } from '@/components/ui/help-tooltip'
import { RestartOverlay } from '@/components/restart-overlay'
import { RestartProvider, useRestart } from '@/lib/restart-context'
import { ExtraParamsDialog } from '@/components/ui/extra-params-dialog'
import { SharePackDialog } from '@/components/share-pack-dialog'
import { TaskConfigCard, Pagination, ModelTable, ModelCardList } from './model/components'
import { useModelTour, useModelFetcher, useModelAutoSave } from './model/hooks'
import { ProviderForm } from './modelProvider/ProviderForm'
import { ProviderList } from './modelProvider/ProviderList'
import type { APIProvider, DeleteConfirmState } from './modelProvider/types'
import { cleanProviderData } from './modelProvider/utils'

// 导入模块化的类型定义和组件
import type { ModelInfo, ProviderConfig, ModelTaskConfig, TaskConfig } from './model/types'

/** Unwrap backend `{ success, config }` envelope to get the actual config */
function unwrapModelConfig(data: unknown): Record<string, unknown> {
  if (data && typeof data === 'object' && 'config' in data) {
    return (data as { config: Record<string, unknown> }).config
  }
  return data as Record<string, unknown>
}

const ADVANCED_MODEL_TASK_NAMES = new Set(['memory', 'learner', 'emoji', 'voice'])
const MODEL_CONFIG_TABS = ['providers', 'models', 'tasks'] as const
type ModelConfigTab = (typeof MODEL_CONFIG_TABS)[number]

function getInitialModelConfigTab(): ModelConfigTab {
  if (typeof window === 'undefined') {
    return 'providers'
  }

  const tab = new URLSearchParams(window.location.search).get('tab')
  return MODEL_CONFIG_TABS.includes(tab as ModelConfigTab) ? (tab as ModelConfigTab) : 'providers'
}

function getRequiredTaskNames(schema: ConfigSchema | null): Set<string> {
  return new Set(
    (schema?.fields ?? [])
      .filter((field) => field.type === 'object' && !field.advanced && !ADVANCED_MODEL_TASK_NAMES.has(field.name))
      .map((field) => field.name)
  )
}

// 主导出组件：包装 RestartProvider
export function ModelConfigPage() {
  return (
    <RestartProvider>
      <ModelConfigPageContent />
    </RestartProvider>
  )
}

// 内部实现组件
function ModelConfigPageContent() {
  const { i18n } = useTranslation()
  const [models, setModels] = useState<ModelInfo[]>([])
  const [providers, setProviders] = useState<string[]>([])
  const [providerConfigs, setProviderConfigs] = useState<ProviderConfig[]>([])
  const [apiProviders, setApiProviders] = useState<APIProvider[]>([])
  const [modelNames, setModelNames] = useState<string[]>([])
  const [taskConfig, setTaskConfig] = useState<ModelTaskConfig | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [autoSaving, setAutoSaving] = useState(false)
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false)
  const [editDialogOpen, setEditDialogOpen] = useState(false)
  const [editingModel, setEditingModel] = useState<ModelInfo | null>(null)
  const [editingIndex, setEditingIndex] = useState<number | null>(null)
  const [extraParamsDialogOpen, setExtraParamsDialogOpen] = useState(false)
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false)
  const [deletingIndex, setDeletingIndex] = useState<number | null>(null)
  const [providerDialogOpen, setProviderDialogOpen] = useState(false)
  const [editingProvider, setEditingProvider] = useState<APIProvider | null>(null)
  const [editingProviderIndex, setEditingProviderIndex] = useState<number | null>(null)
  const [providerDeleteDialogOpen, setProviderDeleteDialogOpen] = useState(false)
  const [deletingProviderIndex, setDeletingProviderIndex] = useState<number | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedModels, setSelectedModels] = useState<Set<number>>(new Set())
  const [selectedProviders, setSelectedProviders] = useState<Set<number>>(new Set())
  const [batchDeleteDialogOpen, setBatchDeleteDialogOpen] = useState(false)
  const [providerBatchDeleteDialogOpen, setProviderBatchDeleteDialogOpen] = useState(false)
  const [testingProviders, setTestingProviders] = useState<Set<string>>(new Set())
  const [testResults, setTestResults] = useState<Map<string, TestConnectionResult>>(new Map())
  const [deleteConfirmState, setDeleteConfirmState] = useState<DeleteConfirmState>({
    isOpen: false,
    providersToDelete: [],
    affectedModels: [],
    pendingProviders: [],
    context: 'auto',
    oldProviders: [],
  })
  const [taskConfigSchema, setTaskConfigSchema] = useState<ConfigSchema | null>(null)
  const taskConfigSchemaRef = useRef<ConfigSchema | null>(null)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [jumpToPage, setJumpToPage] = useState('')
  const [activeTab, setActiveTab] = useState<ModelConfigTab>(getInitialModelConfigTab)
  
  const [advancedModelSettingsVisible, setAdvancedModelSettingsVisible] = useState(false)
  const [advancedTaskSettingsVisible, setAdvancedTaskSettingsVisible] = useState(false)
  const [tourEntryVisible, setTourEntryVisible] = useState(
    () => localStorage.getItem('model-assignment-tour-entry-dismissed') !== 'true'
  )
  
  // 模型 Combobox 状态
  const [modelComboboxOpen, setModelComboboxOpen] = useState(false)
  const providerAutoSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const providersSnapshotRef = useRef<string | null>(null)
  
  // 嵌入模型警告相关状态
  const [embeddingWarningOpen, setEmbeddingWarningOpen] = useState(false)
  const previousEmbeddingModelsRef = useRef<string[]>([])
  const pendingEmbeddingUpdateRef = useRef<{ field: keyof TaskConfig; value: string[] | number } | null>(null)
  
  // 任务配置问题检查状态
  const [invalidModelRefs, setInvalidModelRefs] = useState<{ taskName: string; invalidModels: string[] }[]>([])
  const [emptyTasks, setEmptyTasks] = useState<string[]>([])
  
  // 表单验证错误状态
  const [formErrors, setFormErrors] = useState<{
    name?: string
    api_provider?: string
    model_identifier?: string
  }>({})
  
  const { toast } = useToast()
  const { isRestarting } = useRestart()
  
  // 自动保存 (使用 hook 封装的逻辑)
  const { clearTimers: clearAutoSaveTimers, initialLoadRef, resetSnapshots } = useModelAutoSave({
    models,
    taskConfig,
    onSavingChange: setAutoSaving,
    onUnsavedChange: setHasUnsavedChanges,
  })

  // 检查任务配置问题
  const checkTaskConfigIssues = useCallback((
    taskConf: ModelTaskConfig | null,
    modelList: ModelInfo[],
    schema?: ConfigSchema | null
  ) => {
    if (!taskConf) return
    
    const modelNameSet = new Set(modelList.map(m => m.name))
    const requiredTaskNames = getRequiredTaskNames(schema ?? taskConfigSchemaRef.current)
    const invalidRefs: { taskName: string; invalidModels: string[] }[] = []
    const emptyTaskList: string[] = []
    
    for (const [key, task] of Object.entries(taskConf)) {
      if (!task) continue
      
      // 检查是否有模型
      if (!task.model_list || task.model_list.length === 0) {
        if (requiredTaskNames.has(key)) {
          emptyTaskList.push(key)
        }
        continue
      }
      
      // 检查是否引用了不存在的模型
      const invalid = task.model_list.filter(modelName => !modelNameSet.has(modelName))
      if (invalid.length > 0) {
        invalidRefs.push({ taskName: key, invalidModels: invalid })
      }
    }
    
    setInvalidModelRefs(invalidRefs)
    setEmptyTasks(emptyTaskList)
  }, [])
  
  // 加载配置
  const loadConfig = useCallback(async () => {
    try {
      setLoading(true)
      const [result, schemaResult] = await Promise.all([getModelConfigCached(), getModelConfigSchema()])
      if (!result.success) {
        toast({
          title: '加载失败',
          description: result.error,
          variant: 'destructive',
        })
        setLoading(false)
        return
      }
      const config = unwrapModelConfig(result.data)
      const modelList = (config.models as ModelInfo[]) || []
      setModels(modelList)
      setModelNames(modelList.map((m) => m.name))
      
      const providerList = (config.api_providers as ProviderConfig[]) || []
      setProviders(providerList.map((p) => p.name))
      setProviderConfigs(providerList)
      setApiProviders(providerList.map((provider) => cleanProviderData(provider as APIProvider)))
      providersSnapshotRef.current = JSON.stringify(providerList.map((provider) => cleanProviderData(provider as APIProvider)))
      
      const taskConf = (config.model_task_config as ModelTaskConfig) || null
      setTaskConfig(taskConf)
      resetSnapshots(modelList, taskConf)
      
      // 解析 model_task_config 的 schema
      let nextTaskConfigSchema: ConfigSchema | null = null
      if (schemaResult.success && schemaResult.data) {
        const schema = (schemaResult.data as unknown as Record<string, unknown>).schema as ConfigSchema
        nextTaskConfigSchema = schema.nested?.model_task_config ?? null
        taskConfigSchemaRef.current = nextTaskConfigSchema
        setTaskConfigSchema(nextTaskConfigSchema)
      }
      
      // 检查任务配置问题
      checkTaskConfigIssues(taskConf, modelList, nextTaskConfigSchema)
      
      // 初始化上一次的 embedding 模型列表
      const embeddingModels = taskConf?.embedding?.model_list || []
      previousEmbeddingModelsRef.current = [...embeddingModels]
      setHasUnsavedChanges(false)
      initialLoadRef.current = false
    } catch (error) {
      console.error('加载配置失败:', error)
    } finally {
      setLoading(false)
    }
  }, [initialLoadRef, checkTaskConfigIssues, resetSnapshots])

  // 初始加载
  useEffect(() => {
    loadConfig()
  }, [loadConfig])

  // 获取指定提供商的配置
  const getProviderConfig = useCallback((providerName: string): ProviderConfig | undefined => {
    return providerConfigs.find(p => p.name === providerName)
  }, [providerConfigs])

  // 模型列表获取 (使用 hook 封装的逻辑)
  const {
    availableModels,
    fetchingModels,
    modelFetchError,
    matchedTemplate,
    fetchModelsForProvider,
    clearModels,
  } = useModelFetcher({ getProviderConfig })

  // 当选择的提供商变化时，获取模型列表
  useEffect(() => {
    if (editDialogOpen && editingModel?.api_provider) {
      fetchModelsForProvider(editingModel.api_provider)
    }
  }, [editDialogOpen, editingModel?.api_provider, fetchModelsForProvider])

  const dismissTourEntry = (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation()
    localStorage.setItem('model-assignment-tour-entry-dismissed', 'true')
    setTourEntryVisible(false)
  }

  const syncProviderState = useCallback((nextProviders: APIProvider[]) => {
    const cleanedProviders = nextProviders.map(cleanProviderData)
    setApiProviders(cleanedProviders)
    setProviders(cleanedProviders.map((provider) => provider.name))
    setProviderConfigs(cleanedProviders.map((provider) => ({
      name: provider.name,
      base_url: provider.base_url,
      api_key: provider.api_key,
      client_type: provider.client_type,
      max_retry: provider.max_retry ?? 2,
      timeout: provider.timeout ?? 30,
      retry_interval: provider.retry_interval ?? 10,
    })))
  }, [])

  const removeModelsForProviders = useCallback((
    sourceModels: ModelInfo[],
    sourceTaskConfig: ModelTaskConfig | null,
    removedModels: unknown[],
  ) => {
    const removedModelNames = new Set(
      removedModels
        .map((model) => (typeof model === 'object' && model !== null && 'name' in model ? String((model as Record<string, unknown>).name) : ''))
        .filter(Boolean)
    )
    if (removedModelNames.size === 0) {
      return { models: sourceModels, taskConfig: sourceTaskConfig }
    }

    const nextModels = sourceModels.filter((model) => !removedModelNames.has(model.name))
    if (!sourceTaskConfig) {
      return { models: nextModels, taskConfig: sourceTaskConfig }
    }

    const nextTaskConfig: ModelTaskConfig = {}
    for (const [taskName, task] of Object.entries(sourceTaskConfig)) {
      nextTaskConfig[taskName] = {
        ...task,
        model_list: (task?.model_list || []).filter((modelName) => !removedModelNames.has(modelName)),
      }
    }
    return { models: nextModels, taskConfig: nextTaskConfig }
  }, [])

  const checkDeleteProviderImpact = useCallback(async (
    nextProviders: APIProvider[],
    context: 'auto' | 'manual' = 'auto'
  ) => {
    const oldProviderNames = new Set(apiProviders.map((provider) => provider.name))
    const nextProviderNames = new Set(nextProviders.map((provider) => provider.name))
    const deletedProviders = Array.from(oldProviderNames).filter((name) => !nextProviderNames.has(name))

    if (deletedProviders.length === 0) {
      return { shouldProceed: true }
    }

    const affectedModels = models.filter((model) => deletedProviders.includes(model.api_provider))
    if (affectedModels.length === 0) {
      return { shouldProceed: true }
    }

    setDeleteConfirmState({
      isOpen: true,
      providersToDelete: deletedProviders,
      affectedModels,
      pendingProviders: nextProviders,
      context,
      oldProviders: [...apiProviders],
    })
    return { shouldProceed: false }
  }, [apiProviders, models])

  const saveProviders = useCallback(async (
    nextProviders: APIProvider[],
    context: 'auto' | 'manual' = 'auto',
    affectedModels: unknown[] = []
  ) => {
    const cleanedProviders = nextProviders.map(cleanProviderData)
    const { models: nextModels, taskConfig: nextTaskConfig } = removeModelsForProviders(models, taskConfig, affectedModels)

    if (context === 'auto' && affectedModels.length === 0) {
      const result = await updateModelConfigSection('api_providers', cleanedProviders)
      if (!result.success) {
        throw new Error(result.error || '保存提供商失败')
      }
    } else {
      const resultGet = await getModelConfig()
      if (!resultGet.success) {
        throw new Error(resultGet.error || '加载模型配置失败')
      }
      const config = unwrapModelConfig(resultGet.data)
      config.api_providers = cleanedProviders
      config.models = nextModels.map(cleanModelForSave)
      config.model_task_config = nextTaskConfig
      const resultUpdate = await updateModelConfig(config)
      if (!resultUpdate.success) {
        throw new Error(resultUpdate.error || '保存模型配置失败')
      }
    }

    syncProviderState(cleanedProviders)
    setModels(nextModels)
    setModelNames(nextModels.map((model) => model.name))
    setTaskConfig(nextTaskConfig)
    checkTaskConfigIssues(nextTaskConfig, nextModels)
    providersSnapshotRef.current = JSON.stringify(cleanedProviders)
    setHasUnsavedChanges(false)

  }, [checkTaskConfigIssues, models, removeModelsForProviders, syncProviderState, taskConfig])

  const autoSaveProviders = useCallback(async (nextProviders: APIProvider[]) => {
    if (initialLoadRef.current) return
    const { shouldProceed } = await checkDeleteProviderImpact(nextProviders, 'auto')
    if (!shouldProceed) {
      setHasUnsavedChanges(true)
      return
    }

    try {
      setAutoSaving(true)
      await saveProviders(nextProviders, 'auto')
    } catch (error) {
      console.error('自动保存提供商失败:', error)
      toast({
        title: '自动保存失败',
        description: (error as Error).message,
        variant: 'destructive',
      })
      setHasUnsavedChanges(true)
    } finally {
      setAutoSaving(false)
    }
  }, [checkDeleteProviderImpact, initialLoadRef, saveProviders, toast])

  useEffect(() => {
    if (initialLoadRef.current) return
    const snapshot = JSON.stringify(apiProviders.map(cleanProviderData))
    if (providersSnapshotRef.current === null) {
      providersSnapshotRef.current = snapshot
      return
    }
    if (snapshot === providersSnapshotRef.current) return

    setHasUnsavedChanges(true)
    if (providerAutoSaveTimerRef.current) {
      clearTimeout(providerAutoSaveTimerRef.current)
    }
    providerAutoSaveTimerRef.current = setTimeout(() => {
      autoSaveProviders(apiProviders)
    }, 2000)

    return () => {
      if (providerAutoSaveTimerRef.current) {
        clearTimeout(providerAutoSaveTimerRef.current)
      }
    }
  }, [apiProviders, autoSaveProviders, initialLoadRef])
  
  // 一键删除所有无效模型引用
  const handleRemoveInvalidRefs = useCallback(() => {
    if (!taskConfig) return
    
    const modelNameSet = new Set(models.map(m => m.name))
    const newTaskConfig: ModelTaskConfig = {}
    
    // 遍历所有任务，过滤掉无效的模型引用
    for (const [key, task] of Object.entries(taskConfig)) {
      if (task && task.model_list) {
        newTaskConfig[key] = { ...task, model_list: task.model_list.filter(modelName => modelNameSet.has(modelName)) }
      } else {
        newTaskConfig[key] = task
      }
    }
    
    setTaskConfig(newTaskConfig)
    setInvalidModelRefs([])
    
    toast({
      title: '清理完成',
      description: '已删除所有无效的模型引用',
    })
  }, [taskConfig, models, toast])

  // 清理模型中的 null 值（TOML 不支持 null）
  const cleanModelForSave = (model: ModelInfo): ModelInfo => {
    const cleaned: ModelInfo = {
      model_identifier: model.model_identifier,
      name: model.name,
      api_provider: model.api_provider,
      price_in: model.price_in ?? 0,
      price_out: model.price_out ?? 0,
      cache: model.cache ?? false,
      cache_price_in: model.cache_price_in ?? 0,
      visual: model.visual ?? false,
      force_stream_mode: model.force_stream_mode ?? false,
      extra_params: model.extra_params ?? {},
    }
    // 只有在有值时才添加可选字段
    if (model.temperature != null) {
      cleaned.temperature = model.temperature
    }
    if (model.max_tokens != null) {
      cleaned.max_tokens = model.max_tokens
    }
    return cleaned
  }

  // 保存配置（手动保存）
  const saveConfig = async () => {
    try {
      setSaving(true)
      
      // 先取消自动保存定时器
      clearAutoSaveTimers()
      if (providerAutoSaveTimerRef.current) {
        clearTimeout(providerAutoSaveTimerRef.current)
      }

      const resultGet = await getModelConfig()
      if (!resultGet.success) {
        toast({
          title: '保存失败',
          description: resultGet.error,
          variant: 'destructive',
        })
        setSaving(false)
        return
      }
      const config = unwrapModelConfig(resultGet.data)
      // 清理每个模型中的 null 值
      config.api_providers = apiProviders.map(cleanProviderData)
      config.models = models.map(cleanModelForSave)
      config.model_task_config = taskConfig
      const resultUpdate = await updateModelConfig(config)
      if (!resultUpdate.success) {
        toast({
          title: '保存失败',
          description: resultUpdate.error,
          variant: 'destructive',
        })
        setSaving(false)
        return
      }
      resetSnapshots(config.models as ModelInfo[], taskConfig)
      providersSnapshotRef.current = JSON.stringify(config.api_providers)
      setHasUnsavedChanges(false)
      toast({
        title: '保存成功',
        description: '模型配置已保存',
      })
      await loadConfig() // 重新加载以更新模型名称列表
    } catch (error) {
      console.error('保存配置失败:', error)
      toast({
        title: '保存失败',
        description: (error as Error).message,
        variant: 'destructive',
      })
    } finally {
      setSaving(false)
    }
  }

  // 打开编辑对话框
  const openEditDialog = (model: ModelInfo | null, index: number | null) => {
    // 清除表单验证错误
    setFormErrors({})
    
    setEditingModel(
      model || {
        model_identifier: '',
        name: '',
        api_provider: providers[0] || '',
        price_in: 0,
        price_out: 0,
        cache: false,
        cache_price_in: 0,
        temperature: null,
        max_tokens: null,
        visual: false,
        force_stream_mode: false,
        extra_params: {},
      }
    )
    setAdvancedModelSettingsVisible(false)
    setEditingIndex(index)
    setEditDialogOpen(true)
  }

  const openProviderDialog = (provider: APIProvider | null, index: number | null) => {
    setEditingProvider(provider || {
      name: '',
      base_url: '',
      api_key: '',
      client_type: 'openai',
      max_retry: 2,
      timeout: 30,
      retry_interval: 10,
    })
    setEditingProviderIndex(index)
    setProviderDialogOpen(true)
  }

  const handleActiveTabChange = useCallback((value: string) => {
    const nextTab = MODEL_CONFIG_TABS.includes(value as ModelConfigTab)
      ? (value as ModelConfigTab)
      : 'providers'
    setActiveTab(nextTab)
    const nextUrl = nextTab === 'providers' ? '/config/model' : `/config/model?tab=${nextTab}`
    window.history.replaceState(null, '', nextUrl)
  }, [])

  // Tour 引导 (使用 hook 封装的逻辑)
  const { startTour: handleStartTour, isRunning: tourIsRunning } = useModelTour({
    onOpenEditDialog: () => openEditDialog(null, null),
    onCloseEditDialog: () => setEditDialogOpen(false),
    onOpenProviderDialog: () => openProviderDialog(null, null),
    onCloseProviderDialog: () => setProviderDialogOpen(false),
    onOpenProvidersTab: () => handleActiveTabChange('providers'),
    onOpenModelsTab: () => handleActiveTabChange('models'),
    onOpenTasksTab: () => handleActiveTabChange('tasks'),
  })

  const handleSaveProviderEdit = (provider: APIProvider, index: number | null) => {
    const providerToSave = cleanProviderData(provider)
    if (index !== null) {
      const nextProviders = [...apiProviders]
      nextProviders[index] = providerToSave
      syncProviderState(nextProviders)
    } else {
      syncProviderState([...apiProviders, providerToSave])
    }
    setProviderDialogOpen(false)
    setEditingProvider(null)
    setEditingProviderIndex(null)
    toast({
      title: index !== null ? '提供商已更新' : '提供商已添加',
      description: '配置将在 2 秒后自动保存，或点击右上角"保存配置"按钮立即保存',
    })
  }

  // 保存编辑
  const handleSaveEdit = () => {
    if (!editingModel) return

    // 验证必填项
    const errors: { name?: string; api_provider?: string; model_identifier?: string } = {}
    if (!editingModel.name?.trim()) {
      errors.name = '请输入模型名称'
    } else {
      // 检查名称是否与现有模型重复
      const isDuplicate = models.some((m, index) => {
        // 编辑时排除自身
        if (editingIndex !== null && index === editingIndex) {
          return false
        }
        return m.name.trim().toLowerCase() === editingModel.name.trim().toLowerCase()
      })
      if (isDuplicate) {
        errors.name = '模型名称已存在，请使用其他名称'
      }
    }
    if (!editingModel.api_provider?.trim()) {
      errors.api_provider = '请选择 API 提供商'
    }
    if (!editingModel.model_identifier?.trim()) {
      errors.model_identifier = '请输入模型标识符'
    }

    if (Object.keys(errors).length > 0) {
      setFormErrors(errors)
      return
    }

    // 清除错误状态
    setFormErrors({})

    // 填充空值的默认值，并移除 null 值的可选字段（TOML 不支持 null）
    const modelToSave: ModelInfo = {
      model_identifier: editingModel.model_identifier,
      name: editingModel.name,
      api_provider: editingModel.api_provider,
      price_in: editingModel.price_in ?? 0,
      price_out: editingModel.price_out ?? 0,
      cache: editingModel.cache ?? false,
      cache_price_in: editingModel.cache_price_in ?? 0,
      visual: editingModel.visual ?? false,
      force_stream_mode: editingModel.force_stream_mode ?? false,
      extra_params: editingModel.extra_params ?? {},
    }
    
    // 只有在有值时才添加可选字段
    if (editingModel.temperature != null) {
      modelToSave.temperature = editingModel.temperature
    }
    if (editingModel.max_tokens != null) {
      modelToSave.max_tokens = editingModel.max_tokens
    }

    let newModels: ModelInfo[]
    let oldModelName: string | null = null
    
    if (editingIndex !== null) {
      // 记录旧的模型名称，用于更新任务配置
      oldModelName = models[editingIndex].name
      newModels = [...models]
      newModels[editingIndex] = modelToSave
    } else {
      newModels = [...models, modelToSave]
    }
    
    setModels(newModels)
    setModelNames(newModels.map((m) => m.name))

    // 如果模型名称发生变化，更新任务配置中对该模型的引用
    if (oldModelName && oldModelName !== modelToSave.name && taskConfig) {
      const updateModelList = (list: string[]): string[] => {
        return list.map(name => name === oldModelName ? modelToSave.name : name)
      }
      
      const newTaskConfig: ModelTaskConfig = {}
      for (const [key, task] of Object.entries(taskConfig)) {
        newTaskConfig[key] = { ...task, model_list: updateModelList(task?.model_list || []) }
      }
      setTaskConfig(newTaskConfig)
    }

    setEditDialogOpen(false)
    setEditingModel(null)
    setEditingIndex(null)
    
    // 提示用户配置将自动保存
    toast({
      title: editingIndex !== null ? '模型已更新' : '模型已添加',
      description: '配置将在 2 秒后自动保存，或点击右上角"保存配置"按钮立即保存',
    })
  }

  // 处理编辑对话框关闭
  const handleEditDialogClose = (open: boolean) => {
    if (!open && editingModel) {
      // 关闭时填充默认值
      const updatedModel = {
        ...editingModel,
        price_in: editingModel.price_in ?? 0,
        price_out: editingModel.price_out ?? 0,
      }
      setEditingModel(updatedModel)
    }
    setEditDialogOpen(open)
  }

  // 打开删除确认对话框
  const openDeleteDialog = (index: number) => {
    setDeletingIndex(index)
    setDeleteDialogOpen(true)
  }

  // 确认删除模型
  const handleConfirmDelete = () => {
    if (deletingIndex !== null) {
      const newModels = models.filter((_, i) => i !== deletingIndex)
      setModels(newModels)
      setModelNames(newModels.map((m) => m.name))
      // 重新检查任务配置问题
      checkTaskConfigIssues(taskConfig, newModels)
      toast({
        title: '删除成功',
        description: '配置将在 2 秒后自动保存，或点击右上角"保存配置"按钮立即保存',
      })
    }
    setDeleteDialogOpen(false)
    setDeletingIndex(null)
  }

  const openProviderDeleteDialog = (index: number) => {
    setDeletingProviderIndex(index)
    setProviderDeleteDialogOpen(true)
  }

  const handleConfirmProviderDelete = async () => {
    if (deletingProviderIndex !== null) {
      const nextProviders = apiProviders.filter((_, index) => index !== deletingProviderIndex)
      const { shouldProceed } = await checkDeleteProviderImpact(nextProviders, 'manual')
      if (shouldProceed) {
        syncProviderState(nextProviders)
        toast({
          title: '删除成功',
          description: '提供商已从列表中移除',
        })
      }
    }
    setProviderDeleteDialogOpen(false)
    setDeletingProviderIndex(null)
  }

  const toggleProviderSelection = (index: number) => {
    const nextSelected = new Set(selectedProviders)
    if (nextSelected.has(index)) {
      nextSelected.delete(index)
    } else {
      nextSelected.add(index)
    }
    setSelectedProviders(nextSelected)
  }

  const toggleSelectAllProviders = () => {
    if (selectedProviders.size === apiProviders.length) {
      setSelectedProviders(new Set())
    } else {
      setSelectedProviders(new Set(apiProviders.map((_, index) => index)))
    }
  }

  const openProviderBatchDeleteDialog = () => {
    if (selectedProviders.size === 0) {
      toast({
        title: '提示',
        description: '请先选择要删除的提供商',
        variant: 'default',
      })
      return
    }
    setProviderBatchDeleteDialogOpen(true)
  }

  const handleConfirmProviderBatchDelete = async () => {
    const nextProviders = apiProviders.filter((_, index) => !selectedProviders.has(index))
    const { shouldProceed } = await checkDeleteProviderImpact(nextProviders, 'manual')
    if (shouldProceed) {
      const deletedCount = selectedProviders.size
      syncProviderState(nextProviders)
      setSelectedProviders(new Set())
      toast({
        title: '批量删除成功',
        description: `已删除 ${deletedCount} 个提供商`,
      })
    }
    setProviderBatchDeleteDialogOpen(false)
  }

  const handleConfirmDeleteProviderImpact = async () => {
    try {
      const savingFlag = deleteConfirmState.context === 'auto' ? setAutoSaving : setSaving
      const saveContext = deleteConfirmState.context === 'auto' ? 'auto' : 'manual'
      savingFlag(true)
      await saveProviders(
        deleteConfirmState.pendingProviders,
        saveContext,
        deleteConfirmState.affectedModels
      )
      toast({
        title: '删除成功',
        description: `已删除 ${deleteConfirmState.providersToDelete.length} 个提供商和 ${deleteConfirmState.affectedModels.length} 个关联模型`,
      })
      setDeleteConfirmState({
        isOpen: false,
        providersToDelete: [],
        affectedModels: [],
        pendingProviders: [],
        context: 'auto',
        oldProviders: [],
      })
      setSelectedProviders(new Set())
    } catch (error) {
      toast({
        title: '删除失败',
        description: (error as Error).message,
        variant: 'destructive',
      })
    } finally {
      setSaving(false)
      setAutoSaving(false)
    }
  }

  const handleCancelDeleteProviderImpact = () => {
    if (deleteConfirmState.oldProviders.length > 0) {
      syncProviderState(deleteConfirmState.oldProviders)
    }
    setDeleteConfirmState({
      isOpen: false,
      providersToDelete: [],
      affectedModels: [],
      pendingProviders: [],
      context: 'auto',
      oldProviders: [],
    })
    setHasUnsavedChanges(false)
  }

  const handleTestProviderConnection = async (providerName: string) => {
    setTestingProviders((prev) => new Set(prev).add(providerName))
    try {
      const result = await testProviderConnection(providerName)
      if (!result.success) {
        toast({
          title: '测试失败',
          description: result.error,
          variant: 'destructive',
        })
        return
      }
      const testResult = result.data
      setTestResults((prev) => new Map(prev).set(providerName, testResult))
      if (testResult.network_ok && testResult.api_key_valid !== false) {
        toast({
          title: testResult.api_key_valid === true ? '连接正常' : '网络连接正常',
          description: `${providerName} 可以访问 (${testResult.latency_ms}ms)`,
        })
      } else {
        toast({
          title: testResult.network_ok ? '连接正常但 Key 无效' : '连接失败',
          description: testResult.error || `${providerName} API Key 无效或无法连接`,
          variant: 'destructive',
        })
      }
    } catch (error) {
      toast({
        title: '测试失败',
        description: (error as Error).message,
        variant: 'destructive',
      })
    } finally {
      setTestingProviders((prev) => {
        const next = new Set(prev)
        next.delete(providerName)
        return next
      })
    }
  }

  const handleTestAllProviderConnections = async () => {
    for (const provider of apiProviders) {
      await handleTestProviderConnection(provider.name)
    }
  }

  // 切换单个模型选择
  const toggleModelSelection = (index: number) => {
    const newSelected = new Set(selectedModels)
    if (newSelected.has(index)) {
      newSelected.delete(index)
    } else {
      newSelected.add(index)
    }
    setSelectedModels(newSelected)
  }

  // 全选/取消全选
  const toggleSelectAll = () => {
    if (selectedModels.size === filteredModels.length) {
      setSelectedModels(new Set())
    } else {
      const allIndices = filteredModels.map((_, idx) => 
        models.findIndex(m => m === filteredModels[idx])
      )
      setSelectedModels(new Set(allIndices))
    }
  }

  // 打开批量删除确认对话框
  const openBatchDeleteDialog = () => {
    if (selectedModels.size === 0) {
      toast({
        title: '提示',
        description: '请先选择要删除的模型',
        variant: 'default',
      })
      return
    }
    setBatchDeleteDialogOpen(true)
  }

  // 确认批量删除
  const handleConfirmBatchDelete = () => {
    const deletedCount = selectedModels.size
    const newModels = models.filter((_, index) => !selectedModels.has(index))
    setModels(newModels)
    setModelNames(newModels.map((m) => m.name))
    // 重新检查任务配置问题
    checkTaskConfigIssues(taskConfig, newModels)
    setSelectedModels(new Set())
    setBatchDeleteDialogOpen(false)
    toast({
      title: '批量删除成功',
      description: `已删除 ${deletedCount} 个模型，配置将在 2 秒后自动保存`,
    })
  }

  // 更新任务配置
  const updateTaskConfig = (
    taskName: string,
    field: keyof TaskConfig,
    value: string[] | number | string
  ) => {
    if (!taskConfig) return
    
    // 检测 embedding 模型列表变化
    if (taskName === 'embedding' && field === 'model_list' && Array.isArray(value)) {
      const previousModels = previousEmbeddingModelsRef.current
      const newModels = value as string[]
      
      const hasChanges = 
        previousModels.length !== newModels.length ||
        previousModels.some(model => !newModels.includes(model)) ||
        newModels.some(model => !previousModels.includes(model))
      
      if (hasChanges && previousModels.length > 0) {
        pendingEmbeddingUpdateRef.current = { field, value }
        setEmbeddingWarningOpen(true)
        return
      }
    }
    
    // 正常更新配置
    const newTaskConfig = {
      ...taskConfig,
      [taskName]: {
        ...taskConfig[taskName],
        [field]: value,
      },
    }
    setTaskConfig(newTaskConfig)
    
    // 重新检查任务配置问题
    checkTaskConfigIssues(newTaskConfig, models)
    
    // 如果是 embedding 模型列表，更新 ref
    if (taskName === 'embedding' && field === 'model_list' && Array.isArray(value)) {
      previousEmbeddingModelsRef.current = [...(value as string[])]
    }
  }
  
  // 确认更新嵌入模型
  const handleConfirmEmbeddingChange = () => {
    if (!taskConfig || !pendingEmbeddingUpdateRef.current) return
    
    const { field, value } = pendingEmbeddingUpdateRef.current
    
    const newTaskConfig = {
      ...taskConfig,
      embedding: {
        ...taskConfig.embedding,
        [field]: value,
      },
    }
    setTaskConfig(newTaskConfig)
    
    // 重新检查任务配置问题
    checkTaskConfigIssues(newTaskConfig, models)
    
    // 更新 ref
    if (field === 'model_list' && Array.isArray(value)) {
      previousEmbeddingModelsRef.current = [...(value as string[])]
    }
    
    // 清理
    pendingEmbeddingUpdateRef.current = null
    setEmbeddingWarningOpen(false)
    
    toast({
      title: '嵌入模型已更新',
      description: '建议重新生成知识库向量以确保最佳匹配精度',
    })
  }
  
  // 取消更新嵌入模型
  const handleCancelEmbeddingChange = () => {
    pendingEmbeddingUpdateRef.current = null
    setEmbeddingWarningOpen(false)
  }

  // 过滤模型列表
  const filteredModels = models.filter((model) => {
    if (!searchQuery) return true
    const query = searchQuery.toLowerCase()
    return (
      model.name.toLowerCase().includes(query) ||
      model.model_identifier.toLowerCase().includes(query) ||
      model.api_provider.toLowerCase().includes(query)
    )
  })

  // 分页逻辑
  const totalPages = Math.ceil(filteredModels.length / pageSize)
  const paginatedModels = filteredModels.slice(
    (page - 1) * pageSize,
    page * pageSize
  )

  // 页码跳转
  const handleJumpToPage = () => {
    const targetPage = parseInt(jumpToPage)
    if (targetPage >= 1 && targetPage <= totalPages) {
      setPage(targetPage)
      setJumpToPage('')
    }
  }

  // 检查模型是否被任务使用
  const isModelUsed = (modelName: string): boolean => {
    if (!taskConfig) return false
    return Object.values(taskConfig).some(task => task?.model_list?.includes(modelName))
  }

  if (loading) {
    return (
      <ScrollArea className="h-full">
        <div className="space-y-4 sm:space-y-6 p-4 sm:p-6">
          <div className="flex items-center justify-center h-64">
            <ThinkingIllustration size="lg" />
          </div>
        </div>
      </ScrollArea>
    )
  }

  return (
    <ScrollArea className="h-full">
      <div className="space-y-4 sm:space-y-6 p-4 sm:p-6">
        {/* 无效模型引用警告 */}
        {invalidModelRefs.length > 0 && (
          <Alert variant="destructive">
            <AlertTriangle className="h-4 w-4" />
            <AlertDescription className="flex items-start justify-between gap-4">
              <div className="flex-1">
                <strong>检测到无效的模型引用</strong>
                <div className="mt-2 space-y-1">
                  {invalidModelRefs.map(({ taskName, invalidModels }) => (
                    <div key={taskName} className="text-sm">
                      <strong>{taskName}</strong> 引用了不存在的模型: {invalidModels.join(', ')}
                    </div>
                  ))}
                </div>
              </div>
              <Button
                variant="outline"
                size="sm"
                className="shrink-0 bg-background hover:bg-accent"
                onClick={handleRemoveInvalidRefs}
              >
                一键清理
              </Button>
            </AlertDescription>
          </Alert>
        )}
        
        {/* 空任务警告 */}
        {emptyTasks.length > 0 && (
          <Alert variant="default" className="border-yellow-500/50 bg-yellow-500/10">
            <AlertTriangle className="h-4 w-4 text-yellow-600" />
            <AlertDescription>
              <strong className="text-yellow-600">以下任务未配置模型</strong>
              <div className="mt-2 text-sm">
                {emptyTasks.join('、')} 还未分配模型，这些功能将无法正常工作。
              </div>
            </AlertDescription>
          </Alert>
        )}


        {/* 新手引导入口 - 仅在桌面端显示，移动端隐藏 */}
        {tourEntryVisible && (
        <Alert className="hidden lg:flex border-primary/30 bg-primary/5 cursor-pointer hover:bg-primary/10 transition-colors" onClick={handleStartTour}>
          <GraduationCap className="h-4 w-4 text-primary" />
          <AlertDescription className="flex items-center justify-between">
            <span>
              <strong className="text-primary">新手引导：</strong>不知道如何配置模型？点击这里开始学习如何为麦麦的组件分配模型。
            </span>
            <div className="ml-4 flex shrink-0 items-center gap-2">
            <Button variant="outline" size="sm">
              开始引导
            </Button>
            <Button type="button" variant="ghost" size="sm" onClick={dismissTourEntry}>
              关闭
            </Button>
            </div>
          </AlertDescription>
        </Alert>
        )}

        {/* 标签页 */}
        <Tabs value={activeTab} onValueChange={handleActiveTabChange} className="w-full">
          <div className="flex w-full items-stretch gap-2">
            <TabsList className="grid h-9 min-w-0 flex-1 grid-cols-3">
              <TabsTrigger value="providers" className="w-full" data-tour="providers-tab-trigger">模型厂商设置</TabsTrigger>
              <TabsTrigger value="models" className="w-full" data-tour="models-tab-trigger">模型列表</TabsTrigger>
              <TabsTrigger value="tasks" className="w-full" data-tour="tasks-tab-trigger">为模型分配功能</TabsTrigger>
            </TabsList>
            {activeTab === 'models' && (
              <SharePackDialog
                trigger={
                  <Button variant="outline" size="icon" className="h-9 w-9 shrink-0" aria-label="分享配置">
                    <Share2 className="h-4 w-4" />
                  </Button>
                }
              />
            )}
          </div>
          {/* 模型厂商设置标签页 */}
          <TabsContent value="providers" className="space-y-4 mt-0">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div className="hidden">
                {selectedProviders.size > 0 && (
                  <Button
                    onClick={openProviderBatchDeleteDialog}
                    size="sm"
                    variant="destructive"
                    className="w-full sm:w-auto"
                  >
                    <Trash2 className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                    批量删除 ({selectedProviders.size})
                  </Button>
                )}
                <Button
                  onClick={handleTestAllProviderConnections}
                  size="sm"
                  variant="outline"
                  className="w-full sm:w-auto"
                  disabled={apiProviders.length === 0 || testingProviders.size > 0}
                >
                  <Zap className="mr-2 h-4 w-4" />
                  {testingProviders.size > 0 ? `测试中 (${testingProviders.size})` : '测试全部'}
                </Button>
                <Button onClick={() => openProviderDialog(null, null)} size="sm" variant="outline" className="w-full sm:w-auto">
                  <Plus className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                  添加提供商
                </Button>
              </div>
            </div>

            <ProviderList
              providers={apiProviders}
              testingProviders={testingProviders}
              testResults={testResults}
              selectedProviders={selectedProviders}
              toolbarActions={(
                <>
                  {selectedProviders.size > 0 && (
                    <Button
                      onClick={openProviderBatchDeleteDialog}
                      size="sm"
                      variant="destructive"
                      className="w-full sm:w-auto"
                    >
                      <Trash2 className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                      <span className="text-sm">批量删除 ({selectedProviders.size})</span>
                    </Button>
                  )}
                  <Button
                    onClick={handleTestAllProviderConnections}
                    size="sm"
                    variant="outline"
                    className="w-full sm:w-auto"
                    disabled={apiProviders.length === 0 || testingProviders.size > 0}
                  >
                    <Zap className="mr-2 h-4 w-4" />
                    <span className="text-sm">
                      {testingProviders.size > 0 ? `测试中 (${testingProviders.size})` : '测试全部连接'}
                    </span>
                  </Button>
                  <Button onClick={() => openProviderDialog(null, null)} size="sm" variant="outline" className="w-full sm:w-auto" data-tour="add-provider-button">
                    <Plus className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                    <span className="text-sm">添加厂商</span>
                  </Button>
                </>
              )}
              onEdit={openProviderDialog}
              onDelete={openProviderDeleteDialog}
              onTest={handleTestProviderConnection}
              onToggleSelect={toggleProviderSelection}
              onToggleSelectAll={toggleSelectAllProviders}
            />
          </TabsContent>
          {/* 模型配置标签页 */}
          <TabsContent value="models" className="space-y-4 mt-0">
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2">
              <div className="hidden">
                {selectedModels.size > 0 && (
                  <Button 
                    onClick={openBatchDeleteDialog} 
                    size="sm" 
                    variant="destructive" 
                    className="w-full sm:w-auto"
                  >
                    <Trash2 className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                    批量删除 ({selectedModels.size})
                  </Button>
                )}
                <Button onClick={() => openEditDialog(null, null)} size="sm" variant="outline" className="w-full sm:w-auto">
                  <Plus className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                  添加模型
                </Button>
              </div>
            </div>

          {/* 搜索框 */}
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex w-full min-w-0 flex-col gap-2 sm:flex-1 sm:flex-row sm:items-center">
              <div className="relative w-full sm:max-w-sm">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="搜索模型名称、标识符或提供商..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-9"
                />
              </div>
              {searchQuery && (
                <p className="text-sm text-muted-foreground whitespace-nowrap">
                  找到 {filteredModels.length} 个结果
                </p>
              )}
            </div>

          {/* 模型列表 - 移动端卡片视图 */}
            <div className="flex w-full flex-col gap-2 sm:ml-auto sm:w-auto sm:flex-row sm:items-center sm:justify-end">
              <Button 
                onClick={saveConfig} 
                disabled={saving || autoSaving || !hasUnsavedChanges || isRestarting} 
                size="sm"
                variant="outline"
                className="w-full sm:w-auto sm:min-w-[120px]"
              >
                <Save className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                {saving ? '保存中...' : autoSaving ? '自动保存中...' : hasUnsavedChanges ? '保存配置' : '已保存'}
              </Button>
              {selectedModels.size > 0 && (
                <Button
                  onClick={openBatchDeleteDialog}
                  size="sm"
                  variant="destructive"
                  className="w-full sm:w-auto"
                >
                  <Trash2 className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                  <span className="text-sm">批量删除 ({selectedModels.size})</span>
                </Button>
              )}
              <Button onClick={() => openEditDialog(null, null)} size="sm" variant="outline" className="w-full sm:w-auto" data-tour="add-model-button">
                <Plus className="mr-2 h-4 w-4" strokeWidth={2} fill="none" />
                <span className="text-sm">添加模型</span>
              </Button>
            </div>
          </div>

          <ModelCardList
            paginatedModels={paginatedModels}
            allModels={models}
            onEdit={openEditDialog}
            onDelete={openDeleteDialog}
            isModelUsed={isModelUsed}
            searchQuery={searchQuery}
          />

          {/* 模型列表 - 桌面端表格视图 */}
          <ModelTable
            paginatedModels={paginatedModels}
            allModels={models}
            filteredModels={filteredModels}
            selectedModels={selectedModels}
            onEdit={openEditDialog}
            onDelete={openDeleteDialog}
            onToggleSelection={toggleModelSelection}
            onToggleSelectAll={toggleSelectAll}
            isModelUsed={isModelUsed}
            searchQuery={searchQuery}
          />

          {/* 分页 - 使用模块化组件 */}
          <Pagination
            page={page}
            pageSize={pageSize}
            totalItems={filteredModels.length}
            jumpToPage={jumpToPage}
            onPageChange={setPage}
            onPageSizeChange={setPageSize}
            onJumpToPageChange={setJumpToPage}
            onJumpToPage={handleJumpToPage}
            onSelectionClear={() => setSelectedModels(new Set())}
          />
        </TabsContent>

        {/* 模型任务配置标签页 */}
        <TabsContent value="tasks" className="mt-0 space-y-3">
          <div className="mt-2 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm text-muted-foreground">
              为不同的任务配置使用的模型和参数
            </p>
            {taskConfigSchema?.fields.some((field) => field.advanced) && (
              <Button
                type="button"
                variant={advancedTaskSettingsVisible ? 'default' : 'outline'}
                size="sm"
                onClick={() => setAdvancedTaskSettingsVisible((current) => !current)}
              >
                高级设置
              </Button>
            )}
          </div>

          {taskConfig && taskConfigSchema && (
            <div className="divide-y">
              {taskConfigSchema.fields
                .filter(f => f.type === 'object' && (advancedTaskSettingsVisible || !f.advanced))
                .map((field, index) => {
                  return (
                    <TaskConfigCard
                      key={field.name}
                      title={resolveFieldLabel(field, i18n.language)}
                      description={field.description}
                      taskConfig={taskConfig[field.name] ?? { model_list: [] }}
                      modelNames={modelNames}
                      onChange={(f, value) => updateTaskConfig(field.name, f, value)}
                      advanced={field.advanced}
                      showAdvancedSettings={advancedTaskSettingsVisible}
                      {...(index === 0 ? { dataTour: 'task-model-select' } : {})}
                    />
                  )
                })}
            </div>
          )}
        </TabsContent>
      </Tabs>

      <ProviderForm
        open={providerDialogOpen}
        onOpenChange={setProviderDialogOpen}
        editingProvider={editingProvider}
        editingIndex={editingProviderIndex}
        providers={apiProviders}
        onSave={handleSaveProviderEdit}
        tourState={{ isRunning: tourIsRunning }}
      />

      {/* 删除提供商确认对话框 */}
      <AlertDialog open={providerDeleteDialogOpen} onOpenChange={setProviderDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认删除提供商</AlertDialogTitle>
            <AlertDialogDescription>
              确定要删除提供商"{deletingProviderIndex !== null ? apiProviders[deletingProviderIndex]?.name : ''}"吗？
              如果该提供商下存在模型，确认时会提示一并处理关联模型。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmProviderDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* 批量删除提供商确认对话框 */}
      <AlertDialog open={providerBatchDeleteDialogOpen} onOpenChange={setProviderBatchDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认批量删除提供商</AlertDialogTitle>
            <AlertDialogDescription>
              确定要删除选中的 {selectedProviders.size} 个提供商吗？
              如果这些提供商下存在模型，确认时会提示一并处理关联模型。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmProviderBatchDelete}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              批量删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* 删除提供商影响确认对话框 */}
      <AlertDialog open={deleteConfirmState.isOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-amber-500" />
              删除提供商会同时移除关联模型
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-3 text-sm">
                <p>
                  将删除 {deleteConfirmState.providersToDelete.length} 个提供商，并移除
                  {' '}{deleteConfirmState.affectedModels.length} 个使用这些提供商的模型。
                </p>
                {deleteConfirmState.affectedModels.length > 0 && (
                  <div className="rounded-md bg-muted p-3 text-muted-foreground">
                    {deleteConfirmState.affectedModels.slice(0, 8).map((model) => (
                      <div key={(model as ModelInfo).name}>
                        {(model as ModelInfo).name} ({(model as ModelInfo).api_provider})
                      </div>
                    ))}
                    {deleteConfirmState.affectedModels.length > 8 && (
                      <div>还有 {deleteConfirmState.affectedModels.length - 8} 个模型...</div>
                    )}
                  </div>
                )}
                <p className="font-medium text-foreground">
                  关联模型会从模型列表和任务分配中移除，此操作无法撤销。
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={handleCancelDeleteProviderImpact}>取消</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleConfirmDeleteProviderImpact}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              确认删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* 编辑模型对话框 */}
      <Dialog open={editDialogOpen} onOpenChange={handleEditDialogClose}>
        <DialogContent 
          className="max-w-[95vw] gap-3 p-4 sm:max-w-2xl sm:gap-4 sm:p-6"
          data-tour="model-dialog"
          preventOutsideClose={tourIsRunning}
          confirmOnEnter
        >
          <DialogHeader>
            <DialogTitle>
              {editingIndex !== null ? '编辑模型' : '添加模型'}
            </DialogTitle>
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <DialogDescription>配置模型的基本信息和参数</DialogDescription>
              <Button
                type="button"
                variant={advancedModelSettingsVisible ? 'default' : 'outline'}
                size="sm"
                onClick={() => setAdvancedModelSettingsVisible((current) => !current)}
                className="self-start sm:self-auto"
              >
                高级设置
              </Button>
            </div>
          </DialogHeader>

          <DialogBody viewportClassName="min-h-0 flex-1 pr-3 sm:pr-4 [&>div]:!block">
          <div className="grid gap-3 py-2 sm:gap-4 sm:py-4">
            <div className="grid gap-2" data-tour="model-name-input">
              <div className="flex flex-col gap-1.5 sm:flex-row sm:items-center sm:gap-2">
                <Label
                  htmlFor="model_name"
                  className={`sm:w-28 sm:flex-shrink-0 ${formErrors.name ? 'text-destructive' : ''}`}
                >
                  模型名称 *
                </Label>
                <Input
                  id="model_name"
                  value={editingModel?.name || ''}
                  onChange={(e) => {
                    setEditingModel((prev) =>
                      prev ? { ...prev, name: e.target.value } : null
                    )
                    if (formErrors.name) {
                      setFormErrors((prev) => ({ ...prev, name: undefined }))
                    }
                  }}
                  placeholder="例如: qwen3-30b"
                  className={`sm:flex-1 ${formErrors.name ? 'border-destructive focus-visible:ring-destructive' : ''}`}
                />
              </div>
              {formErrors.name ? (
                <p className="text-xs text-destructive sm:pl-28">{formErrors.name}</p>
              ) : null}
            </div>

            <div className="grid gap-2" data-tour="model-provider-select">
              <div className="flex flex-col gap-1.5 sm:flex-row sm:items-center sm:gap-2">
                <Label
                  htmlFor="api_provider"
                  className={`sm:w-28 sm:flex-shrink-0 ${formErrors.api_provider ? 'text-destructive' : ''}`}
                >
                  API 提供商 *
                </Label>
                <Select
                  value={editingModel?.api_provider || ''}
                  onValueChange={(value) => {
                    setEditingModel((prev) =>
                      prev ? { ...prev, api_provider: value } : null
                    )
                    // 清空模型列表和错误状态，等待 useEffect 重新获取
                    clearModels()
                    if (formErrors.api_provider) {
                      setFormErrors((prev) => ({ ...prev, api_provider: undefined }))
                    }
                  }}
                >
                  <SelectTrigger id="api_provider" className={`sm:flex-1 ${formErrors.api_provider ? 'border-destructive focus-visible:ring-destructive' : ''}`}>
                    <SelectValue placeholder="选择提供商" />
                  </SelectTrigger>
                  <SelectContent>
                    {providers.map((provider) => (
                      <SelectItem key={provider} value={provider}>
                        {provider}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              {formErrors.api_provider && (
                <p className="text-xs text-destructive sm:pl-28">{formErrors.api_provider}</p>
              )}
            </div>

            <div className="grid gap-2" data-tour="model-identifier-input">
              <div className="flex items-center justify-between">
                <Label htmlFor="model_identifier" className={formErrors.model_identifier ? 'text-destructive' : ''}>模型标识符 *</Label>
                {matchedTemplate?.modelFetcher && (
                  <div className="flex items-center gap-2">
                    <Badge variant="secondary" className="text-xs">
                      {matchedTemplate.display_name}
                    </Badge>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 px-2"
                      onClick={() => editingModel?.api_provider && fetchModelsForProvider(editingModel.api_provider, true)}
                      disabled={fetchingModels}
                    >
                      {fetchingModels ? (
                        <Loader2 className="h-3 w-3 animate-spin" />
                      ) : (
                        <RefreshCw className="h-3 w-3" />
                      )}
                    </Button>
                  </div>
                )}
              </div>
              
              <div className="flex flex-col gap-1.5 sm:flex-row sm:gap-2">
                {/* 模型标识符 Combobox */}
                {matchedTemplate?.modelFetcher && (
                  <Popover open={modelComboboxOpen} onOpenChange={setModelComboboxOpen}>
                    <PopoverTrigger asChild>
                      <Button
                        variant="outline"
                        role="combobox"
                        aria-expanded={modelComboboxOpen}
                        className="w-full justify-between font-normal sm:w-[46%]"
                        disabled={fetchingModels || !!modelFetchError}
                      >
                        {fetchingModels ? (
                          <span className="flex items-center gap-2 text-muted-foreground">
                            <Loader2 className="h-4 w-4 animate-spin" />
                            正在获取模型列表...
                          </span>
                        ) : modelFetchError ? (
                          <span className="text-muted-foreground text-sm">手动填写</span>
                        ) : editingModel?.model_identifier ? (
                          <span className="truncate">{editingModel.model_identifier}</span>
                        ) : (
                          <span className="text-muted-foreground">搜索或选择模型...</span>
                        )}
                        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="p-0" align="start" style={{ width: 'var(--radix-popover-trigger-width)' }}>
                      <Command>
                        <CommandInput placeholder="搜索模型..." />
                        <CommandList className="max-h-[300px]">
                          <CommandEmpty>
                            {modelFetchError ? (
                              <div className="py-4 px-2 text-center space-y-2">
                                <p className="text-sm text-destructive">{modelFetchError}</p>
                                {!modelFetchError.includes('API Key') && (
                                  <Button
                                    variant="link"
                                    size="sm"
                                    onClick={() => editingModel?.api_provider && fetchModelsForProvider(editingModel.api_provider, true)}
                                  >
                                    重试
                                  </Button>
                                )}
                              </div>
                            ) : (
                              '未找到匹配的模型'
                            )}
                          </CommandEmpty>
                          <CommandGroup heading="可用模型">
                            {availableModels.map((model) => (
                              <CommandItem
                                key={model.id}
                                value={model.id}
                                className="pr-8"
                                onSelect={() => {
                                  setEditingModel((prev) =>
                                    prev ? { ...prev, model_identifier: model.id } : null
                                  )
                                  setModelComboboxOpen(false)
                                }}
                              >
                                {editingModel?.model_identifier === model.id && (
                                  <Check className="absolute right-2 h-4 w-4" />
                                )}
                                <div className="flex min-w-0 flex-col">
                                  <span className="truncate">{model.id}</span>
                                  {model.name !== model.id && (
                                    <span className="truncate text-xs text-muted-foreground">{model.name}</span>
                                  )}
                                </div>
                              </CommandItem>
                            ))}
                          </CommandGroup>
                        </CommandList>
                      </Command>
                    </PopoverContent>
                  </Popover>
                )}

                <Input
                  id="model_identifier"
                  value={editingModel?.model_identifier || ''}
                  onChange={(e) => {
                    setEditingModel((prev) =>
                      prev ? { ...prev, model_identifier: e.target.value } : null
                    )
                    if (formErrors.model_identifier) {
                      setFormErrors((prev) => ({ ...prev, model_identifier: undefined }))
                    }
                  }}
                  placeholder={matchedTemplate?.modelFetcher ? '手动输入模型标识符' : 'Qwen/Qwen3-30B-A3B-Instruct-2507'}
                  className={`${matchedTemplate?.modelFetcher ? 'sm:flex-1' : 'w-full'} ${formErrors.model_identifier ? 'border-destructive focus-visible:ring-destructive' : ''}`}
                />
              </div>
              
              {/* 表单验证错误提示 */}
              {formErrors.model_identifier && (
                <p className="text-xs text-destructive">{formErrors.model_identifier}</p>
              )}
              
              {/* 模型获取错误提示 */}
              {modelFetchError && matchedTemplate?.modelFetcher && !formErrors.model_identifier && (
                <Alert variant="destructive" className="mt-2 py-2">
                  <Info className="h-4 w-4" />
                  <AlertDescription className="text-xs">
                    {modelFetchError}
                  </AlertDescription>
                </Alert>
              )}
              
              {!formErrors.model_identifier && (
                <p className="text-xs text-muted-foreground">
                  {modelFetchError 
                    ? '请手动输入模型标识符，或前往"模型厂商设置"检查 API Key'
                    : matchedTemplate?.modelFetcher 
                      ? `已识别为 ${matchedTemplate.display_name}，支持自动获取模型列表` 
                      : 'API 提供商提供的模型 ID'}
                </p>
              )}
            </div>

            <div className="flex flex-wrap items-center gap-x-4 gap-y-2 sm:gap-6">
              <div className="flex items-center space-x-2">
                <Switch
                  id="model_visual"
                  checked={editingModel?.visual || false}
                  onCheckedChange={(checked) =>
                    setEditingModel((prev) =>
                      prev ? { ...prev, visual: checked } : null
                    )
                  }
                />
                <Label htmlFor="model_visual" className="cursor-pointer">
                  启用视觉
                </Label>
              </div>

              <div className="flex items-center space-x-2">
                <Switch
                  id="model_cache"
                  checked={editingModel?.cache || false}
                  onCheckedChange={(checked) =>
                    setEditingModel((prev) =>
                      prev ? { ...prev, cache: checked } : null
                    )
                  }
                />
                <Label htmlFor="model_cache" className="cursor-pointer">
                  支持缓存
                </Label>
              </div>
            </div>

            <div className={`grid grid-cols-1 gap-3 sm:gap-4 ${editingModel?.cache ? 'md:grid-cols-3' : 'sm:grid-cols-2'}`}>
              <div className="grid gap-2">
                <Label htmlFor="price_in">输入价格 (¥/M token)</Label>
                <Input
                  id="price_in"
                  type="number"
                  step="0.1"
                  min="0"
                  value={editingModel?.price_in ?? ''}
                  onChange={(e) => {
                    const val = e.target.value === '' ? null : parseFloat(e.target.value)
                    setEditingModel((prev) =>
                      prev
                        ? { ...prev, price_in: val }
                        : null
                    )
                  }}
                  placeholder="默认: 0"
                />
              </div>

              <div className="grid gap-2">
                <Label htmlFor="price_out">输出价格 (¥/M token)</Label>
                <Input
                  id="price_out"
                  type="number"
                  step="0.1"
                  min="0"
                  value={editingModel?.price_out ?? ''}
                  onChange={(e) => {
                    const val = e.target.value === '' ? null : parseFloat(e.target.value)
                    setEditingModel((prev) =>
                      prev
                        ? { ...prev, price_out: val }
                        : null
                    )
                  }}
                  placeholder="默认: 0"
                />
              </div>

              {editingModel?.cache && (
                <div className="grid gap-2">
                  <Label htmlFor="cache_price_in">缓存价格 (¥/M token)</Label>
                  <Input
                    id="cache_price_in"
                    type="number"
                    step="0.1"
                    min="0"
                    value={editingModel?.cache_price_in ?? ''}
                    onChange={(e) => {
                      const val = e.target.value === '' ? null : parseFloat(e.target.value)
                      setEditingModel((prev) =>
                        prev
                          ? { ...prev, cache_price_in: val }
                          : null
                      )
                    }}
                    placeholder="默认: 0"
                  />
                </div>
              )}
            </div>

            {advancedModelSettingsVisible && (
              <div className="space-y-3 rounded-lg border border-amber-200 bg-amber-50/50 p-3 dark:border-amber-500/40 dark:bg-amber-500/10 sm:space-y-4 sm:p-4">
                <div className="flex items-center justify-between gap-4">
                  <div className="space-y-1">
                    <Label htmlFor="force_stream_mode" className="cursor-pointer">强制流式输出模式</Label>
                    <p className="text-xs text-muted-foreground">
                      用于必须通过流式响应返回内容的模型
                    </p>
                  </div>
                  <Switch
                    id="force_stream_mode"
                    checked={editingModel?.force_stream_mode || false}
                    onCheckedChange={(checked) =>
                      setEditingModel((prev) =>
                        prev ? { ...prev, force_stream_mode: checked } : null
                      )
                    }
                  />
                </div>
              </div>
            )}

            {/* 模型级别温度 */}
            <div className="space-y-2 rounded-lg border p-3 sm:space-y-3 sm:p-4">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <div className="flex items-center gap-1.5">
                    <Label htmlFor="enable_model_temperature" className="cursor-pointer">自定义模型温度</Label>
                    <HelpTooltip
                      content={
                        <div className="space-y-2">
                          <p className="font-medium">什么是温度（Temperature）？</p>
                          <p>温度控制模型输出的随机性和创造性：</p>
                          <ul className="list-disc list-inside space-y-1 text-xs">
                            <li><strong>低温度（0.1-0.3）</strong>：更确定、更保守的输出，适合事实性任务</li>
                            <li><strong>中温度（0.5-0.7）</strong>：平衡创造性与可控性</li>
                            <li><strong>高温度（0.8-1.0）</strong>：更有创意、更多样化的输出</li>
                            <li><strong>极高温度（1.0-2.0）</strong>：极度随机，可能产生不可预测的结果</li>
                          </ul>
                        </div>
                      }
                      side="right"
                      maxWidth="400px"
                    />
                  </div>
                  <p className="text-xs text-muted-foreground">
                    启用后将覆盖「为模型分配功能」中的任务温度配置
                  </p>
                </div>
                <Switch
                  id="enable_model_temperature"
                  checked={editingModel?.temperature != null}
                  onCheckedChange={(checked) => {
                    if (checked) {
                      setEditingModel((prev) => prev ? { ...prev, temperature: 0.7 } : null)
                    } else {
                      setEditingModel((prev) => prev ? { ...prev, temperature: null } : null)
                    }
                  }}
                />
              </div>
              
              {editingModel?.temperature != null && (
                <div className="space-y-3 pt-2 border-t">
                  <div className="flex items-center justify-between gap-3">
                    <Label className="text-sm">温度值</Label>
                    <Input
                      type="number"
                      value={editingModel.temperature}
                      onChange={(e) => {
                        const value = parseFloat(e.target.value)
                        if (!isNaN(value) && value >= 0 && value <= 2) {
                          setEditingModel((prev) => prev ? { ...prev, temperature: value } : null)
                        }
                      }}
                      onBlur={(e) => {
                        const value = parseFloat(e.target.value)
                        if (isNaN(value) || value < 0) {
                          setEditingModel((prev) => prev ? { ...prev, temperature: 0 } : null)
                        } else if (value > 2) {
                          setEditingModel((prev) => prev ? { ...prev, temperature: 2 } : null)
                        }
                      }}
                      step={0.01}
                      min={0}
                      max={2}
                      className="w-20 h-8 text-sm text-right tabular-nums"
                    />
                  </div>
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-muted-foreground tabular-nums">0</span>
                    <Slider
                      value={[editingModel.temperature]}
                      onValueChange={(values) =>
                        setEditingModel((prev) =>
                          prev ? { ...prev, temperature: values[0] } : null
                        )
                      }
                      min={0}
                      max={2}
                      step={0.05}
                      className="flex-1"
                    />
                    <span className="text-xs text-muted-foreground tabular-nums">2</span>
                  </div>
                  {editingModel.temperature > 1 && (
                    <Alert className="bg-amber-500/10 border-amber-500/20 [&>svg+div]:translate-y-0">
                      <AlertTriangle className="h-4 w-4 text-amber-500" />
                      <AlertDescription className="text-xs text-amber-600 dark:text-amber-400">
                        温度 &gt; 1 会产生更随机、更不可预测的输出，请谨慎使用
                      </AlertDescription>
                    </Alert>
                  )}
                  <p className="text-xs text-muted-foreground">
                    较低（0.1-0.5）产生确定输出，中等（0.5-1.0）平衡创造性，较高（1.0-2.0）产生极度随机输出
                  </p>
                </div>
              )}
            </div>

            {/* 模型级别最大 Token */}
            <div className="space-y-2 rounded-lg border p-3 sm:space-y-3 sm:p-4">
              <div className="flex items-center justify-between">
                <div className="space-y-0.5">
                  <div className="flex items-center gap-1.5">
                    <Label htmlFor="enable_model_max_tokens" className="cursor-pointer">自定义最大 Token</Label>
                    <HelpTooltip
                      content={
                        <div className="space-y-2">
                          <p className="font-medium">什么是最大 Token？</p>
                          <p>控制模型单次回复的最大长度。1 token ≈ 0.75 个英文单词或 0.5 个中文字符。</p>
                          <ul className="list-disc list-inside space-y-1 text-xs">
                            <li><strong>较小值（512-1024）</strong>：简短回复，节省成本</li>
                            <li><strong>中等值（2048-4096）</strong>：正常对话长度</li>
                            <li><strong>较大值（8192+）</strong>：长文本生成，成本较高</li>
                          </ul>
                        </div>
                      }
                      side="right"
                      maxWidth="400px"
                    />
                  </div>
                  <p className="text-xs text-muted-foreground">
                    启用后将覆盖「为模型分配功能」中的任务最大 Token 配置
                  </p>
                </div>
                <Switch
                  id="enable_model_max_tokens"
                  checked={editingModel?.max_tokens != null}
                  onCheckedChange={(checked) => {
                    if (checked) {
                      // 启用时设置默认值 2048
                      setEditingModel((prev) => prev ? { ...prev, max_tokens: 2048 } : null)
                    } else {
                      // 禁用时清除
                      setEditingModel((prev) => prev ? { ...prev, max_tokens: null } : null)
                    }
                  }}
                />
              </div>
              
              {editingModel?.max_tokens != null && (
                <div className="space-y-2 pt-2 border-t">
                  <div className="flex items-center justify-between">
                    <Label className="text-sm">最大 Token 数</Label>
                    <Input
                      type="number"
                      min="1"
                      max="128000"
                      value={editingModel.max_tokens}
                      onChange={(e) => {
                        const val = parseInt(e.target.value)
                        if (!isNaN(val) && val >= 1) {
                          setEditingModel((prev) => prev ? { ...prev, max_tokens: val } : null)
                        }
                      }}
                      className="w-28 h-8 text-sm"
                    />
                  </div>
                  <p className="text-xs text-muted-foreground">
                    限制模型单次输出的最大 token 数量，不同模型支持的上限不同
                  </p>
                </div>
              )}
            </div>

            {/* 额外参数 */}
            <div className="space-y-2">
              <Label className="text-sm font-medium">额外参数</Label>
              <div className="flex items-center gap-2">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="flex-1 justify-start h-9"
                  onClick={() => setExtraParamsDialogOpen(true)}
                >
                  <Settings className="h-4 w-4 mr-2" />
                  {Object.keys(editingModel?.extra_params || {}).length > 0 ? (
                    <span>
                      已配置 {Object.keys(editingModel?.extra_params || {}).length} 个参数
                    </span>
                  ) : (
                    <span className="text-muted-foreground">未配置额外参数</span>
                  )}
                </Button>
              </div>
              {Object.keys(editingModel?.extra_params || {}).length > 0 && (
                <div className="text-xs text-muted-foreground px-1">
                  {Object.keys(editingModel?.extra_params || {})
                    .slice(0, 3)
                    .map((key) => (
                      <span key={key} className="inline-block mr-2">
                        <code className="px-1.5 py-0.5 bg-muted rounded">{key}</code>
                      </span>
                    ))}
                  {Object.keys(editingModel?.extra_params || {}).length > 3 && (
                    <span>...</span>
                  )}
                </div>
              )}
            </div>
          </div>
          </DialogBody>

          <DialogFooter className="flex-row justify-end gap-2 space-x-0">
            <Button
              variant="outline"
              className="flex-1 sm:flex-none"
              onClick={() => setEditDialogOpen(false)}
              data-tour="model-cancel-button"
            >
              取消
            </Button>
            <Button
              data-dialog-action="confirm"
              className="flex-1 sm:flex-none"
              onClick={handleSaveEdit}
              data-tour="model-save-button"
            >
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 删除确认对话框 */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认删除</AlertDialogTitle>
            <AlertDialogDescription>
              确定要删除模型 "{deletingIndex !== null ? models[deletingIndex]?.name : ''}" 吗？
              此操作无法撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmDelete}>删除</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* 批量删除确认对话框 */}
      <AlertDialog open={batchDeleteDialogOpen} onOpenChange={setBatchDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>确认批量删除</AlertDialogTitle>
            <AlertDialogDescription>
              确定要删除选中的 {selectedModels.size} 个模型吗？
              此操作无法撤销。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>取消</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmBatchDelete} className="bg-destructive text-destructive-foreground hover:bg-destructive/90">
              批量删除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* 嵌入模型更换警告对话框 */}
      <AlertDialog open={embeddingWarningOpen} onOpenChange={setEmbeddingWarningOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-amber-500" />
              更换嵌入模型警告
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-3 text-sm">
                <p>
                  <strong className="text-foreground">注意：</strong>更换嵌入模型可能会影响知识库的匹配精度！
                </p>
                <ul className="space-y-2 ml-4 list-disc text-muted-foreground">
                  <li>不同的嵌入模型会产生不同的向量表示</li>
                  <li>这可能导致现有知识库的检索结果不准确</li>
                  <li>建议更换嵌入模型后重新生成所有知识库的向量</li>
                </ul>
                <p className="text-foreground font-medium">
                  确定要更换嵌入模型吗？
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={handleCancelEmbeddingChange}>取消</AlertDialogCancel>
            <AlertDialogAction 
              onClick={handleConfirmEmbeddingChange}
              className="bg-amber-600 hover:bg-amber-700"
            >
              确认更换
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* 额外参数编辑弹窗 */}
      <ExtraParamsDialog
        open={extraParamsDialogOpen}
        onOpenChange={setExtraParamsDialogOpen}
        value={editingModel?.extra_params || {}}
        onChange={(params) =>
          setEditingModel((prev) =>
            prev ? { ...prev, extra_params: params } : null
          )
        }
      />

      {/* 重启遮罩层 */}
      <RestartOverlay />
      </div>
    </ScrollArea>
  )
}
