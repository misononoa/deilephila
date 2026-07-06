<script setup lang="ts">
import { onMounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { useAsyncAction } from "../composables/useAsyncAction";
import { truncateHex } from "../utils";
import type { FollowData } from "../types";

const emit = defineEmits<{ changed: [] }>();

const follows = ref<FollowData[]>([]);
const pubkeyInput = ref("");
const { error, loading, run } = useAsyncAction();

async function refresh() {
  follows.value = await invoke<FollowData[]>("get_follows");
}

async function follow() {
  const pubkey = pubkeyInput.value.trim();
  if (!pubkey || loading.value) return;
  await run(async () => {
    await invoke("follow_user", { pubkey });
    pubkeyInput.value = "";
    await refresh();
    emit("changed");
  });
}

async function unfollow(pubkey: string) {
  if (loading.value) return;
  await run(async () => {
    await invoke("unfollow_user", { pubkey });
    await refresh();
    emit("changed");
  });
}

onMounted(refresh);
</script>

<template>
  <section class="bg-white border border-gray-200 rounded-xl p-4 flex flex-col gap-3">
    <form class="flex gap-2" @submit.prevent="follow">
      <input
        v-model="pubkeyInput"
        placeholder="フォローする公開鍵(64桁の16進)"
        :disabled="loading"
        class="flex-1 bg-white text-gray-900 placeholder:text-gray-400 outline-none text-sm font-mono border border-gray-200 rounded-lg px-3 py-1.5"
      />
      <button
        type="submit"
        :disabled="loading || pubkeyInput.trim().length === 0"
        class="bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white rounded-lg px-4 py-1.5 text-sm font-medium cursor-pointer shrink-0"
      >
        フォロー
      </button>
    </form>
    <p v-if="error" class="text-sm text-red-500">{{ error }}</p>
    <ul v-if="follows.length > 0" class="flex flex-col gap-1">
      <li
        v-for="f in follows"
        :key="f.pubkey"
        class="flex items-center justify-between gap-2 text-sm"
      >
        <span class="truncate" :title="f.pubkey">
          <template v-if="f.display_name">
            {{ f.display_name }}
            <span class="font-mono text-xs text-gray-400 ml-1">{{ truncateHex(f.pubkey) }}</span>
          </template>
          <span v-else class="font-mono text-xs text-gray-500">{{ truncateHex(f.pubkey) }}</span>
        </span>
        <button
          :disabled="loading"
          class="text-xs text-gray-400 hover:text-red-500 cursor-pointer shrink-0"
          @click="unfollow(f.pubkey)"
        >
          解除
        </button>
      </li>
    </ul>
    <p v-else class="text-xs text-gray-400">まだ誰もフォローしていません</p>
  </section>
</template>
