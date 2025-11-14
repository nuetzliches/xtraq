<script setup lang="ts">
const { footer } = useAppConfig()
</script>

<template>
  <UFooter>
    <template #left>
      <div class="flex items-center gap-3">
        <NuxtLink
          v-if="footer?.logo?.light || footer?.logo?.dark"
          :to="'/'"
          class="inline-flex items-center"
        >
          <UColorModeImage
            :light="footer?.logo?.light || ''"
            :dark="footer?.logo?.dark || ''"
            :alt="footer?.logo?.alt || 'xtraq logo'"
            class="h-6 w-auto"
          />
        </NuxtLink>

        <span>
          {{ footer.credits }}
        </span>
      </div>
    </template>

    <template #right>
      <UColorModeButton v-if="footer?.colorMode" />

      <template v-if="footer?.links">
        <UButton
          v-for="(link, index) of footer?.links"
          :key="index"
          v-bind="{ color: 'neutral', variant: 'ghost', ...link }"
        />
      </template>
    </template>
  </UFooter>
</template>
