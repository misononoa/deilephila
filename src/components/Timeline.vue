<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import PostCard from "./PostCard.vue";
import type { ForkData, PostData } from "../types";

const posts = ref<PostData[]>([]);
const loading = ref(false);
// fork(equivocation)が記録されたアカウント。該当投稿に警告バッジを出す
const forkedAuthors = ref<Set<string>>(new Set());

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

async function refreshForks() {
  const forks = await invoke<ForkData[]>("get_forks");
  forkedAuthors.value = new Set(forks.map((f) => f.author));
}

// バックエンドが新規イベントを取り込むと timeline-updated が emit される。
// 必ずリスナー登録 → 初回 refresh の順にする: 登録完了後の emit はイベントで拾い、
// 登録前に完了した同期(unlock 直後の DHT 回収など)は初回 refresh が DB から読む。
// 逆順だと refresh とリスナー登録の隙間に emit が落ちて再描画されない
let unlisten: UnlistenFn | null = null;
let unlistenFork: UnlistenFn | null = null;

onMounted(async () => {
  unlisten = await listen("timeline-updated", () => {
    refresh();
  });
  unlistenFork = await listen("fork-detected", () => {
    refreshForks();
  });
  await refresh();
  await refreshForks();
});

onUnmounted(() => {
  unlisten?.();
  unlistenFork?.();
});

defineExpose({ refresh });
</script>
<template>
  <div class="flex flex-col gap-3">
    <div v-if="loading && posts.length === 0" class="text-center text-gray-400 py-8 text-sm">読み込み中…</div>
    <div v-else-if="posts.length === 0" class="text-center text-gray-400 py-8 text-sm">
      まだ投稿がありません
    </div>
    <PostCard
      v-for="post in visiblePosts"
      :key="post.cid"
      :post="post"
      :forked="forkedAuthors.has(post.author)"
    />
  </div>
</template>
