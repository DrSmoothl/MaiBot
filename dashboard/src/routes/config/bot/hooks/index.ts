/**
 * Bot 配置页面相关 hooks
 */

export { useAutoSave, useAutoSaveGeneric, useConfigAutoSave } from './useAutoSave'
export type {
  UseAutoSaveOptions,
  UseAutoSaveReturn,
  AutoSaveState,
  UseAutoSaveConfig,
  UseAutoSaveReturnGeneric,
} from './useAutoSave'
export {
  AliasNamesHook,
  AMemorixSharedMemoryGroupsHook,
  BotPlatformsHook,
  BotPlatformAccountsHook,
  ChatPromptsHook,
  ChatTalkValueRulesHook,
  ExpressionGroupsHook,
  ExpressionLearningListHook,
  JargonGroupsHook,
  JargonLearningListHook,
  KeywordRulesHook,
  HiddenFieldHook,
  MCPRootItemsHook,
  MCPServersHook,
  MultipleReplyStyleHook,
  RegexRulesHook,
} from './complexFieldHooks'
export { ChatSectionHook } from './ChatSectionHook'
export { PersonalitySectionHook } from './PersonalitySectionHook'
export { DebugSectionHook } from './DebugSectionHook'
export { BotInfoSectionHook } from './BotInfoSectionHook'
