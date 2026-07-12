<script setup lang="ts">
import { computed } from "vue";
import type { PostData } from "../types";
import { truncateHex } from "../utils";

const props = defineProps<{ post: PostData; forked?: boolean }>();

const shortAuthor = computed(() => truncateHex(props.post.author));
const authorName = computed(() => props.post.author_display_name);

const formattedTime = computed(() => {
  const d = new Date(props.post.timestamp);
  return d.toLocaleString("ja-JP", {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
});
</script>

<template>
  <article
    class="bg-white border border-gray-200 rounded-xl p-4"
    :class="{ 'opacity-50': post.deleted }"
  >
    <div class="flex items-center gap-2 mb-2">
      <span v-if="authorName" class="text-xs font-medium text-gray-700" :title="post.author">
        {{ authorName }}
      </span>
      <span class="font-mono text-xs text-gray-400" :title="post.author">{{ shortAuthor }}</span>
      <span class="text-xs text-gray-400">{{ formattedTime }}</span>
      <span v-if="post.edited" class="text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-500">
        編集済み
      </span>
      <span v-if="post.deleted" class="text-xs px-1.5 py-0.5 rounded bg-red-50 text-red-400">
        削除済み
      </span>
      <span
        v-if="forked"
        class="text-xs px-1.5 py-0.5 rounded bg-amber-50 text-amber-600"
        title="このアカウントのチェーンに矛盾する分岐(fork)が検出されています。鍵の漏洩や不正の可能性があります"
      >
        ⚠ fork 検出
      </span>
    </div>
    <p class="text-sm text-gray-800 leading-relaxed whitespace-pre-wrap wrap-break-word">
      {{ post.deleted ? "(削除された投稿)" : post.text }}
    </p>
  </article>
</template>
