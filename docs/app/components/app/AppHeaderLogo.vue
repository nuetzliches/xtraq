<script setup lang="ts">
import { computed } from 'vue'

const appConfig = useAppConfig()
const title = computed(() => appConfig.docus.header?.title ?? '')
const logo = computed(() => appConfig.docus.header?.logo)
const hasLogo = computed(() => Boolean(logo.value?.dark || logo.value?.light))
</script>

<template>
  <div class="flex items-center gap-1.5">
    <UColorModeImage
      v-if="hasLogo"
      :light="logo?.light ?? logo?.dark ?? ''"
      :dark="logo?.dark ?? logo?.light ?? ''"
      :alt="title"
      class="h-8 w-auto shrink-0"
    />
    <span v-if="title" class="font-semibold text-highlighted">
      {{ title }}
    </span>
  </div>
</template>
