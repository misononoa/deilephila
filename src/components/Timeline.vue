<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import PostCard from "./PostCard.vue";
import type { PostData } from "../types";

const posts = ref<PostData[]>([]);
const loading = ref(false);

const visiblePosts = computed(() =>
  posts.value.filter((p) => !p.deleted)
);

async function refresh() {
  loading.value = true;
  try {
    posts.value = await invoke<PostData[]>("get_timeline");
  } finally {
    loading.value = false;
  }
}

// バックエンドの core タスクが新規イベントを取り込むと timeline-updated が emit される
let unlisten: UnlistenFn | null = null;

onMounted(async () => {
  await refresh();
  unlisten = await listen("timeline-updated", () => {
    refresh();
  });
});

onUnmounted(() => {
  unlisten?.();
});

defineExpose({ refresh });
</script>
<template>
  <div class="flex flex-col gap-3">
    <div v-if="loading && posts.length === 0" class="text-center text-gray-400 py-8 text-sm">読み込み中…</div>
    <div v-else-if="posts.length === 0" class="text-center text-gray-400 py-8 text-sm">
      まだ投稿がありません
    </div>
    <PostCard v-for="post in visiblePosts" :key="post.cid" :post="post" />
  </div>
</template>
