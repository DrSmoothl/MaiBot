/**
 * 主题配置的 localStorage 存储管理模块
 * 统一处理主题相关的存储操作，包括加载、保存、导出、导入和迁移旧 key
 */

import { DEFAULT_ACCENT_COLOR_HSL, normalizeAccentColor } from './palette'
import {
  DEFAULT_DASHBOARD_STYLE,
  DEFAULT_DASHBOARD_STYLE_CONFIG,
  DEFAULT_FUTURE_RETRO_STYLE_CONFIG,
} from './tokens'
import type {
  BackgroundConfigMap,
  DashboardStyle,
  DashboardStyleConfig,
  StyleBackgroundConfigMap,
  StyleCustomCSS,
  StyleTokenOverrides,
  ThemeTokens,
  UserThemeConfig,
} from './tokens'

/**
 * 主题存储 key 定义
 * 统一使用 'maibot-theme-*' 前缀，替代现有的 'ui-theme'、'maibot-ui-theme' 和 'accent-color'
 */
export const THEME_STORAGE_KEYS = {
  MODE: 'maibot-theme-mode',
  PRESET: 'maibot-theme-preset',
  ACCENT: 'maibot-theme-accent',
  OVERRIDES: 'maibot-theme-overrides',
  STYLE_OVERRIDES: 'maibot-theme-style-overrides',
  CUSTOM_CSS: 'maibot-theme-custom-css',
  STYLE_CUSTOM_CSS: 'maibot-theme-style-custom-css',
  BACKGROUND_CONFIG: 'maibot-theme-background',
  STYLE_BACKGROUND_CONFIG: 'maibot-theme-style-background',
  DASHBOARD_STYLE: 'maibot-theme-dashboard-style',
  STYLE_CONFIG: 'maibot-theme-style-config',
} as const

/**
 * 默认主题配置
 */
const DEFAULT_THEME_CONFIG: UserThemeConfig = {
  selectedPreset: 'light',
  accentColor: DEFAULT_ACCENT_COLOR_HSL,
  tokenOverrides: {},
  styleTokenOverrides: {},
  customCSS: '',
  styleCustomCSS: {},
  backgroundConfig: {} as BackgroundConfigMap,
  styleBackgroundConfig: {},
  dashboardStyle: DEFAULT_DASHBOARD_STYLE,
  styleConfig: DEFAULT_DASHBOARD_STYLE_CONFIG,
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null

const REMOVED_LAYOUT_TOKEN_KEYS = ['space-unit', 'max-content-width']

const LEGACY_GLOBAL_LAYOUT_TOKEN_KEYS = [
  'space-unit',
  'max-content-width',
  'sidebar-collapsed-width',
  'sidebar-logo-height',
  'sidebar-logo-padding-x',
  'sidebar-nav-padding',
  'sidebar-nav-padding-collapsed',
  'sidebar-section-gap',
  'sidebar-section-title-height',
  'sidebar-section-title-margin-bottom',
  'sidebar-section-title-margin-bottom-collapsed',
  'sidebar-nav-item-gap',
  'sidebar-nav-item-height',
  'sidebar-nav-item-padding-x',
  'sidebar-nav-item-collapsed-width',
]

function stripLayoutTokens(
  tokenOverrides: unknown,
  layoutTokenKeys: string[]
): UserThemeConfig['tokenOverrides'] {
  if (!isRecord(tokenOverrides)) {
    return {}
  }

  const nextOverrides = { ...(tokenOverrides as UserThemeConfig['tokenOverrides']) }
  if (!nextOverrides.layout) {
    return nextOverrides
  }

  const nextLayoutOverrides = { ...nextOverrides.layout }
  for (const key of layoutTokenKeys) {
    delete nextLayoutOverrides[key as keyof typeof nextLayoutOverrides]
  }

  nextOverrides.layout = nextLayoutOverrides
  return nextOverrides
}

function stripLegacyGlobalLayoutTokens(tokenOverrides: unknown): UserThemeConfig['tokenOverrides'] {
  return stripLayoutTokens(tokenOverrides, LEGACY_GLOBAL_LAYOUT_TOKEN_KEYS)
}

function stripRemovedLayoutTokens(tokenOverrides: unknown): UserThemeConfig['tokenOverrides'] {
  return stripLayoutTokens(tokenOverrides, REMOVED_LAYOUT_TOKEN_KEYS)
}

function normalizeStyleTokenOverrides(value: unknown): StyleTokenOverrides {
  if (!isRecord(value)) {
    return {}
  }

  const nextOverrides: StyleTokenOverrides = {}
  for (const style of ['modern', 'future-retro'] as const) {
    if (isRecord(value[style])) {
      nextOverrides[style] = stripRemovedLayoutTokens(value[style])
    }
  }

  return nextOverrides
}

function normalizeStyleCustomCSS(value: unknown): StyleCustomCSS {
  if (!isRecord(value)) {
    return {}
  }

  const nextCustomCSS: StyleCustomCSS = {}
  for (const style of ['modern', 'future-retro'] as const) {
    if (typeof value[style] === 'string') {
      nextCustomCSS[style] = value[style]
    }
  }

  return nextCustomCSS
}

function normalizeStyleBackgroundConfig(value: unknown): StyleBackgroundConfigMap {
  if (!isRecord(value)) {
    return {}
  }

  const nextBackgroundConfig: StyleBackgroundConfigMap = {}
  for (const style of ['modern', 'future-retro'] as const) {
    if (isRecord(value[style])) {
      nextBackgroundConfig[style] = value[style] as BackgroundConfigMap
    }
  }

  return nextBackgroundConfig
}

function normalizeDashboardStyle(value: unknown): DashboardStyle {
  if (value === 'modern' || value === 'future-retro') {
    return value
  }

  return DEFAULT_DASHBOARD_STYLE
}

function normalizeStyleConfig(value: unknown): DashboardStyleConfig {
  const config = isRecord(value) ? value : {}
  const futureRetro = isRecord(config.futureRetro) ? config.futureRetro : {}

  return {
    futureRetro: {
      paperTexture:
        typeof futureRetro.paperTexture === 'boolean'
          ? futureRetro.paperTexture
          : DEFAULT_FUTURE_RETRO_STYLE_CONFIG.paperTexture,
      strongBorders:
        typeof futureRetro.strongBorders === 'boolean'
          ? futureRetro.strongBorders
          : DEFAULT_FUTURE_RETRO_STYLE_CONFIG.strongBorders,
    },
  }
}

/**
 * 从 localStorage 加载完整主题配置
 * 缺失值使用合理默认值
 *
 * @returns 加载的主题配置对象
 */
export function loadThemeConfig(): UserThemeConfig {
  const preset = localStorage.getItem(THEME_STORAGE_KEYS.PRESET)
  const accent = localStorage.getItem(THEME_STORAGE_KEYS.ACCENT)
  const overridesStr = localStorage.getItem(THEME_STORAGE_KEYS.OVERRIDES)
  const styleOverridesStr = localStorage.getItem(THEME_STORAGE_KEYS.STYLE_OVERRIDES)
  const customCSS = localStorage.getItem(THEME_STORAGE_KEYS.CUSTOM_CSS)
  const styleCustomCSSStr = localStorage.getItem(THEME_STORAGE_KEYS.STYLE_CUSTOM_CSS)
  const dashboardStyle = localStorage.getItem(THEME_STORAGE_KEYS.DASHBOARD_STYLE)
  const backgroundConfigStr = localStorage.getItem(THEME_STORAGE_KEYS.BACKGROUND_CONFIG)
  const styleBackgroundConfigStr = localStorage.getItem(THEME_STORAGE_KEYS.STYLE_BACKGROUND_CONFIG)
  const normalizedDashboardStyle = normalizeDashboardStyle(dashboardStyle)

  // 解析 tokenOverrides JSON
  let tokenOverrides: Partial<ThemeTokens> = {}
  if (overridesStr) {
    try {
      const parsedOverrides = JSON.parse(overridesStr)
      tokenOverrides = stripLegacyGlobalLayoutTokens(parsedOverrides)
      if (JSON.stringify(parsedOverrides) !== JSON.stringify(tokenOverrides)) {
        localStorage.setItem(THEME_STORAGE_KEYS.OVERRIDES, JSON.stringify(tokenOverrides))
      }
    } catch {
      // JSON 解析失败，使用空对象
      tokenOverrides = {}
    }
  }

  let styleTokenOverrides: StyleTokenOverrides = {}
  if (styleOverridesStr) {
    try {
      styleTokenOverrides = normalizeStyleTokenOverrides(JSON.parse(styleOverridesStr))
    } catch {
      styleTokenOverrides = {}
    }
  }

  if (Object.keys(tokenOverrides).length > 0) {
    styleTokenOverrides = {
      ...styleTokenOverrides,
      [normalizedDashboardStyle]: {
        ...styleTokenOverrides[normalizedDashboardStyle],
        ...tokenOverrides,
      },
    }
    tokenOverrides = {}
    localStorage.setItem(THEME_STORAGE_KEYS.OVERRIDES, JSON.stringify(tokenOverrides))
    localStorage.setItem(THEME_STORAGE_KEYS.STYLE_OVERRIDES, JSON.stringify(styleTokenOverrides))
  }

  let styleCustomCSS: StyleCustomCSS = {}
  if (styleCustomCSSStr) {
    try {
      styleCustomCSS = normalizeStyleCustomCSS(JSON.parse(styleCustomCSSStr))
    } catch {
      styleCustomCSS = {}
    }
  }

  let legacyCustomCSS = customCSS || ''
  if (legacyCustomCSS.trim().length > 0) {
    styleCustomCSS = {
      ...styleCustomCSS,
      [normalizedDashboardStyle]: styleCustomCSS[normalizedDashboardStyle] ?? legacyCustomCSS,
    }
    legacyCustomCSS = ''
    localStorage.setItem(THEME_STORAGE_KEYS.CUSTOM_CSS, legacyCustomCSS)
    localStorage.setItem(THEME_STORAGE_KEYS.STYLE_CUSTOM_CSS, JSON.stringify(styleCustomCSS))
  }

  let backgroundConfig: BackgroundConfigMap = {}
  if (backgroundConfigStr) {
    try {
      backgroundConfig = JSON.parse(backgroundConfigStr)
    } catch {
      backgroundConfig = {}
    }
  }

  let styleBackgroundConfig: StyleBackgroundConfigMap = {}
  if (styleBackgroundConfigStr) {
    try {
      styleBackgroundConfig = normalizeStyleBackgroundConfig(JSON.parse(styleBackgroundConfigStr))
    } catch {
      styleBackgroundConfig = {}
    }
  }

  if (Object.keys(backgroundConfig).length > 0) {
    styleBackgroundConfig = {
      ...styleBackgroundConfig,
      [normalizedDashboardStyle]: styleBackgroundConfig[normalizedDashboardStyle] ?? backgroundConfig,
    }
    backgroundConfig = {}
    localStorage.setItem(THEME_STORAGE_KEYS.BACKGROUND_CONFIG, JSON.stringify(backgroundConfig))
    localStorage.setItem(
      THEME_STORAGE_KEYS.STYLE_BACKGROUND_CONFIG,
      JSON.stringify(styleBackgroundConfig)
    )
  }

  const styleConfigStr = localStorage.getItem(THEME_STORAGE_KEYS.STYLE_CONFIG)
  let styleConfig: DashboardStyleConfig = DEFAULT_THEME_CONFIG.styleConfig
  if (styleConfigStr) {
    try {
      styleConfig = normalizeStyleConfig(JSON.parse(styleConfigStr))
    } catch {
      styleConfig = DEFAULT_THEME_CONFIG.styleConfig
    }
  }

  return {
    selectedPreset: preset || DEFAULT_THEME_CONFIG.selectedPreset,
    accentColor: normalizeAccentColor(accent),
    tokenOverrides,
    styleTokenOverrides,
    customCSS: legacyCustomCSS || DEFAULT_THEME_CONFIG.customCSS,
    styleCustomCSS,
    backgroundConfig,
    styleBackgroundConfig,
    dashboardStyle: normalizedDashboardStyle,
    styleConfig,
  }
}

/**
 * 保存完整主题配置到 localStorage
 *
 * @param config - 要保存的主题配置
 */
export function saveThemeConfig(config: UserThemeConfig): void {
  localStorage.setItem(THEME_STORAGE_KEYS.PRESET, config.selectedPreset)
  localStorage.setItem(THEME_STORAGE_KEYS.ACCENT, normalizeAccentColor(config.accentColor))
  localStorage.setItem(THEME_STORAGE_KEYS.OVERRIDES, JSON.stringify(config.tokenOverrides))
  localStorage.setItem(
    THEME_STORAGE_KEYS.STYLE_OVERRIDES,
    JSON.stringify(normalizeStyleTokenOverrides(config.styleTokenOverrides))
  )
  localStorage.setItem(THEME_STORAGE_KEYS.CUSTOM_CSS, config.customCSS)
  localStorage.setItem(
    THEME_STORAGE_KEYS.STYLE_CUSTOM_CSS,
    JSON.stringify(normalizeStyleCustomCSS(config.styleCustomCSS))
  )
  localStorage.setItem(
    THEME_STORAGE_KEYS.DASHBOARD_STYLE,
    normalizeDashboardStyle(config.dashboardStyle)
  )
  localStorage.setItem(
    THEME_STORAGE_KEYS.STYLE_CONFIG,
    JSON.stringify(normalizeStyleConfig(config.styleConfig))
  )
  if (config.backgroundConfig) {
    localStorage.setItem(
      THEME_STORAGE_KEYS.BACKGROUND_CONFIG,
      JSON.stringify(config.backgroundConfig)
    )
  } else {
    localStorage.removeItem(THEME_STORAGE_KEYS.BACKGROUND_CONFIG)
  }
  localStorage.setItem(
    THEME_STORAGE_KEYS.STYLE_BACKGROUND_CONFIG,
    JSON.stringify(normalizeStyleBackgroundConfig(config.styleBackgroundConfig))
  )
}

/**
 * 部分更新主题配置
 * 先加载现有配置，合并部分更新，再保存
 *
 * @param partial - 部分主题配置更新
 */
export function saveThemePartial(partial: Partial<UserThemeConfig>): void {
  const current = loadThemeConfig()
  const updated: UserThemeConfig = {
    ...current,
    ...partial,
  }
  saveThemeConfig(updated)
}

/**
 * 导出主题配置为美化格式的 JSON 字符串
 *
 * @returns 格式化的 JSON 字符串
 */
export function exportThemeJSON(): string {
  const config = loadThemeConfig()
  return JSON.stringify(config, null, 2)
}

/**
 * 从 JSON 字符串导入主题配置
 * 包含基础的格式和字段校验
 *
 * @param json - JSON 字符串
 * @returns 导入结果，包含成功状态和错误列表
 */
export function importThemeJSON(json: string): { success: boolean; errors: string[] } {
  const errors: string[] = []

  // JSON 格式校验
  let config: unknown
  try {
    config = JSON.parse(json)
  } catch (error) {
    return {
      success: false,
      errors: [`Invalid JSON format: ${error instanceof Error ? error.message : 'Unknown error'}`],
    }
  }

  // 基本对象类型校验
  if (typeof config !== 'object' || config === null) {
    return {
      success: false,
      errors: ['Configuration must be a JSON object'],
    }
  }

  const configObj = config as Record<string, unknown>

  // 必要字段存在性校验
  if (typeof configObj.selectedPreset !== 'string') {
    errors.push('selectedPreset must be a string')
  }
  if (typeof configObj.accentColor !== 'string') {
    errors.push('accentColor must be a string')
  }
  if (typeof configObj.customCSS !== 'string') {
    errors.push('customCSS must be a string')
  }
  if (
    configObj.styleCustomCSS !== undefined &&
    (typeof configObj.styleCustomCSS !== 'object' || configObj.styleCustomCSS === null)
  ) {
    errors.push('styleCustomCSS must be an object')
  }
  if (configObj.tokenOverrides !== undefined && typeof configObj.tokenOverrides !== 'object') {
    errors.push('tokenOverrides must be an object')
  }
  if (
    configObj.styleTokenOverrides !== undefined &&
    (typeof configObj.styleTokenOverrides !== 'object' || configObj.styleTokenOverrides === null)
  ) {
    errors.push('styleTokenOverrides must be an object')
  }
  if (configObj.dashboardStyle !== undefined && typeof configObj.dashboardStyle !== 'string') {
    errors.push('dashboardStyle must be a string')
  }
  if (
    configObj.styleConfig !== undefined &&
    (typeof configObj.styleConfig !== 'object' || configObj.styleConfig === null)
  ) {
    errors.push('styleConfig must be an object')
  }
  if (
    configObj.styleBackgroundConfig !== undefined &&
    (typeof configObj.styleBackgroundConfig !== 'object' ||
      configObj.styleBackgroundConfig === null)
  ) {
    errors.push('styleBackgroundConfig must be an object')
  }

  if (errors.length > 0) {
    return { success: false, errors }
  }

  // 校验通过，保存配置
  const validConfig: UserThemeConfig = {
    selectedPreset: configObj.selectedPreset as string,
    accentColor: configObj.accentColor as string,
    tokenOverrides: {},
    styleTokenOverrides: normalizeStyleTokenOverrides(
      configObj.styleTokenOverrides ?? {
        [normalizeDashboardStyle(configObj.dashboardStyle)]: configObj.tokenOverrides,
      }
    ),
    customCSS: '',
    styleCustomCSS: normalizeStyleCustomCSS(
      configObj.styleCustomCSS ?? {
        [normalizeDashboardStyle(configObj.dashboardStyle)]: configObj.customCSS,
      }
    ),
    backgroundConfig: {},
    styleBackgroundConfig: normalizeStyleBackgroundConfig(
      configObj.styleBackgroundConfig ?? {
        [normalizeDashboardStyle(configObj.dashboardStyle)]: configObj.backgroundConfig,
      }
    ),
    dashboardStyle: normalizeDashboardStyle(configObj.dashboardStyle),
    styleConfig: normalizeStyleConfig(configObj.styleConfig),
  }

  saveThemeConfig(validConfig)
  return { success: true, errors: [] }
}

/**
 * 重置主题配置为默认值
 * 删除所有 THEME_STORAGE_KEYS 对应的 localStorage 项
 */
export function resetThemeToDefault(): void {
  Object.values(THEME_STORAGE_KEYS).forEach((key) => {
    localStorage.removeItem(key)
  })
}

/**
 * 迁移旧的 localStorage key 到新 key
 * 处理：
 * - 'ui-theme' 或 'maibot-ui-theme' → 'maibot-theme-mode'
 * - 'accent-color' → 'maibot-theme-accent'
 * 迁移完成后删除旧 key，避免重复迁移
 */
export function migrateOldKeys(): void {
  // 迁移主题模式
  // 优先使用 'ui-theme'（因为 ThemeProvider 默认使用它）
  const uiTheme = localStorage.getItem('ui-theme')
  const maiTheme = localStorage.getItem('maibot-ui-theme')
  const newMode = localStorage.getItem(THEME_STORAGE_KEYS.MODE)

  if (!newMode) {
    if (uiTheme) {
      localStorage.setItem(THEME_STORAGE_KEYS.MODE, uiTheme)
    } else if (maiTheme) {
      localStorage.setItem(THEME_STORAGE_KEYS.MODE, maiTheme)
    }
  }

  // 迁移强调色
  const accentColor = localStorage.getItem('accent-color')
  const newAccent = localStorage.getItem(THEME_STORAGE_KEYS.ACCENT)

  if (accentColor && !newAccent) {
    localStorage.setItem(THEME_STORAGE_KEYS.ACCENT, normalizeAccentColor(accentColor))
  } else if (newAccent) {
    const normalizedAccent = normalizeAccentColor(newAccent)
    if (normalizedAccent !== newAccent) {
      localStorage.setItem(THEME_STORAGE_KEYS.ACCENT, normalizedAccent)
    }
  }

  // 删除旧 key
  localStorage.removeItem('ui-theme')
  localStorage.removeItem('maibot-ui-theme')
  localStorage.removeItem('accent-color')
}
