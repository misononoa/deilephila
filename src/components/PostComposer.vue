<script setup lang="ts">
import { ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { useAsyncAction } from "../composables/useAsyncAction";

const emit = defineEmits<{ posted: [] }>();

const text = ref("");
const { error, loading, run } = useAsyncAction();

async function submit() {
  const trimmed = text.value.trim();
  if (!trimmed || loading.value) return;
  await run(async () => {
    await invoke("create_post", { text: trimmed });
    text.value = "";
    emit("posted");
  });
}
</script>

<template>
  <form
    class="bg-white border border-gray-200 rounded-xl p-4 flex flex-col gap-3"
    @submit.prevent="submit"
  >
    <textarea
      v-model="text"
      placeholder="いまどうしてる？"
      rows="3"
      :disabled="loading"
      class="resize-none bg-white text-gray-900 placeholder:text-gray-400 outline-none text-sm leading-relaxed w-full"
      @keydown.ctrl.enter="submit"
      @keydown.meta.enter="submit"
    />
    <div class="flex items-center justify-between">
      <span class="text-xs text-gray-400">Ctrl+Enter で投稿</span>
      <button
        type="submit"
        :disabled="loading || text.trim().length === 0"
        class="bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white rounded-lg px-4 py-1.5 text-sm font-medium cursor-pointer"
      >
        {{ loading ? "投稿中…" : "投稿" }}
      </button>
    </div>
    <p v-if="error" class="text-sm text-red-500">{{ error }}</p>
  </form>
</template>
