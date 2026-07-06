<script setup lang="ts">
import { ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { useAsyncAction } from "../composables/useAsyncAction";

const emit = defineEmits<{ done: [pubkeyHex: string] }>();

const passphrase = ref("");
const confirm = ref("");
const { error, loading, run } = useAsyncAction();

async function submit() {
  if (passphrase.value.length < 1) {
    error.value = "パスフレーズを入力してください";
    return;
  }
  if (passphrase.value !== confirm.value) {
    error.value = "パスフレーズが一致しません";
    return;
  }
  await run(async () => {
    const pubkey = await invoke<string>("setup_account", {
      passphrase: passphrase.value,
    });
    emit("done", pubkey);
  });
}
</script>

<template>
  <div class="flex flex-col items-center justify-center h-full gap-5 p-8 bg-gray-50">
    <h1 class="text-3xl font-bold text-gray-900">deilephila</h1>
    <p class="text-sm text-gray-500">新しいアカウントを作成します</p>
    <form class="flex flex-col gap-3 w-full max-w-xs" @submit.prevent="submit">
      <label class="flex flex-col gap-1 text-sm text-gray-600">
        パスフレーズ
        <input
          v-model="passphrase"
          type="password"
          placeholder="8文字以上を推奨"
          autocomplete="new-password"
          :disabled="loading"
          class="bg-white border border-gray-300 rounded-lg px-3 py-2 text-gray-900 outline-none focus:border-gray-500 disabled:opacity-50"
        />
      </label>
      <label class="flex flex-col gap-1 text-sm text-gray-600">
        確認
        <input
          v-model="confirm"
          type="password"
          placeholder="もう一度入力"
          autocomplete="new-password"
          :disabled="loading"
          class="bg-white border border-gray-300 rounded-lg px-3 py-2 text-gray-900 outline-none focus:border-gray-500 disabled:opacity-50"
        />
      </label>
      <p v-if="error" class="text-sm text-red-500">{{ error }}</p>
      <button
        type="submit"
        :disabled="loading"
        class="bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white rounded-lg px-4 py-2 font-medium text-sm cursor-pointer"
      >
        {{ loading ? "作成中…" : "アカウントを作成" }}
      </button>
    </form>
  </div>
</template>
