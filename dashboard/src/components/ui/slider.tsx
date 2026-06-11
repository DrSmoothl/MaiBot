import * as React from "react"
import * as SliderPrimitive from "@radix-ui/react-slider"

import { cn } from "@/lib/utils"

type SliderProps = React.ComponentPropsWithoutRef<typeof SliderPrimitive.Root> & {
  "data-dashboard-slider"?: "config"
  "data-dashboard-slider-value-format"?: "fixed-2"
}

const Slider = React.forwardRef<
  React.ElementRef<typeof SliderPrimitive.Root>,
  SliderProps
>(({
  className,
  value,
  defaultValue,
  "data-dashboard-slider": dashboardSliderStyle,
  "data-dashboard-slider-value-format": dashboardValueFormat,
  ...props
}, ref) => {
  const displaysThumbValue = dashboardSliderStyle === 'config'
  const currentValues = Array.isArray(value)
    ? value
    : Array.isArray(defaultValue)
      ? defaultValue
      : []
  const thumbCount = Array.isArray(value)
    ? value.length
    : Array.isArray(defaultValue)
      ? defaultValue.length
      : 1

  return (
    <SliderPrimitive.Root
      ref={ref}
      className={cn(
        "relative flex w-full touch-none select-none items-center",
        className
      )}
      value={value}
      defaultValue={defaultValue}
      data-dashboard-slider={dashboardSliderStyle}
      data-dashboard-slider-value-format={dashboardValueFormat}
      {...props}
    >
      <SliderPrimitive.Track
        className={cn(
          "relative h-1.5 w-full grow overflow-hidden rounded-full bg-primary/20",
          displaysThumbValue && "h-3 rounded-none"
        )}
      >
        <SliderPrimitive.Range className="absolute h-full bg-primary" />
      </SliderPrimitive.Track>
      {Array.from({ length: Math.max(1, thumbCount) }).map((_, index) => (
        <SliderPrimitive.Thumb
          key={index}
          className={cn(
            "block h-4 w-4 rounded-full border border-primary/50 bg-background shadow transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50",
            displaysThumbValue &&
              "inline-flex h-7 min-w-9 items-center justify-center rounded-none border-2 border-primary bg-background px-1 text-xs font-semibold leading-none text-foreground"
          )}
        >
          {displaysThumbValue && (
            <span className="pointer-events-none select-none">
              {dashboardValueFormat === 'fixed-2' && typeof currentValues[index] === 'number'
                ? currentValues[index].toFixed(2)
                : currentValues[index]}
            </span>
          )}
        </SliderPrimitive.Thumb>
      ))}
    </SliderPrimitive.Root>
  )
})
Slider.displayName = SliderPrimitive.Root.displayName

export { Slider }
