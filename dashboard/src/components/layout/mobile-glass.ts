// 液态玻璃材质：只用在悬浮层上，半透明磨砂 + 顶部细高光 + 轻投影。
// Tailwind 只识别源码里完整写出的类名，所以「仅手机生效」的版本也逐个写出。
export const MOBILE_GLASS_CLASS =
  'bg-background/70 backdrop-blur-2xl backdrop-saturate-150 shadow-[inset_0_0_0_0.5px_hsl(var(--foreground)/0.08),inset_0_1px_0_rgb(255_255_255/0.55),0_10px_30px_rgb(0_0_0/0.08),0_1px_2px_rgb(0_0_0/0.04)] dark:shadow-[inset_0_0_0_0.5px_rgb(255_255_255/0.12),inset_0_1px_0_rgb(255_255_255/0.08),0_10px_30px_rgb(0_0_0/0.4)]'

export const MOBILE_ONLY_GLASS_CLASS =
  'max-lg:bg-background/70 max-lg:backdrop-blur-2xl max-lg:backdrop-saturate-150 max-lg:shadow-[inset_0_0_0_0.5px_hsl(var(--foreground)/0.08),inset_0_1px_0_rgb(255_255_255/0.55),0_10px_30px_rgb(0_0_0/0.08),0_1px_2px_rgb(0_0_0/0.04)] max-lg:dark:shadow-[inset_0_0_0_0.5px_rgb(255_255_255/0.12),inset_0_1px_0_rgb(255_255_255/0.08),0_10px_30px_rgb(0_0_0/0.4)]'
