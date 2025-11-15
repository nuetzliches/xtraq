<script setup lang="ts">
import { computed } from 'vue'

const appConfig = useAppConfig()
const title = computed(() => appConfig.header?.title ?? '')
</script>

<template>
  <div v-if="appConfig.header?.logo?.dark || appConfig.header?.logo?.light"
       class="flex items-center gap-2">
    <UColorModeImage :light="appConfig.header?.logo?.light || appConfig.header?.logo?.dark"
                     :dark="appConfig.header?.logo?.dark || appConfig.header?.logo?.light"
                     :alt="appConfig.header?.logo?.alt || title || 'Logo'"
                     class="h-6 w-auto shrink-0" />
    <span v-if="title"
          class="text-base font-semibold text-gray-900 dark:text-gray-100">
      {{ title }}
    </span>
  </div>
  <span v-else>
    {{ title || '{appConfig.header.title}' }}
  </span>
</template>
