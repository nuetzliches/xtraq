<script setup lang="ts">
import type { ContentNavigationItem } from '@nuxt/content'

const navigation = inject<Ref<ContentNavigationItem[]>>('navigation')

const { header } = useAppConfig()
</script>

<template>
  <UHeader :ui="{ center: 'flex-1' }">
    <UContentSearchButton v-if="header?.search"
                          :collapsed="false"
                          class="w-full" />

    <template v-if="header?.logo?.dark || header?.logo?.light || header?.title"
              #title>
      <NuxtLink :to="header?.to || '/'"
                class="inline-flex items-center gap-3 text-inherit no-underline">
        <UColorModeImage v-if="header?.logo?.dark || header?.logo?.light"
                         :light="header?.logo?.light!"
                         :dark="header?.logo?.dark!"
                         :alt="header?.logo?.alt"
                         class="h-8 w-8 shrink-0" />

        <span v-if="header?.title"
              class="font-semibold tracking-tight">
          {{ header.title }}
        </span>
      </NuxtLink>
    </template>

    <template v-else
              #left>
      <NuxtLink :to="header?.to || '/'">
        <AppLogo class="w-auto h-6 shrink-0" />
      </NuxtLink>
    </template>

    <template #right>
      <UContentSearchButton v-if="header?.search"
                            class="lg:hidden" />

      <UColorModeButton v-if="header?.colorMode" />

      <template v-if="header?.links">
        <UButton v-for="(link, index) of header.links"
                 :key="index"
                 v-bind="{ color: 'neutral', variant: 'ghost', ...link }" />
      </template>
    </template>

    <template #body>
      <UContentNavigation highlight
                          :navigation="navigation" />
    </template>
  </UHeader>
</template>
