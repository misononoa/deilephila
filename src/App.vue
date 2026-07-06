<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import AccountSetup from "./components/AccountSetup.vue";
import AccountUnlock from "./components/AccountUnlock.vue";
import FollowManager from "./components/FollowManager.vue";
import PostComposer from "./components/PostComposer.vue";
import Timeline from "./components/Timeline.vue";
import { truncateHex } from "./utils";

type AppStatus = "loading" | "not_setup" | "locked" | "unlocked";

interface AppStatusResponse {
  setup: boolean;
  unlocked: boolean;
}

const appStatus = ref<AppStatus>("loading");
const pubkeyHex = ref("");
const timeline = ref<InstanceType<typeof Timeline> | null>(null);

const shortPubkey = computed(() =>
  pubkeyHex.value ? truncateHex(pubkeyHex.value) : ""
);

onMounted(async () => {
  const status = await invoke<AppStatusResponse>("get_app_status");
  if (!status.setup) {
    appStatus.value = "not_setup";
  } else if (!status.unlocked) {
    appStatus.value = "locked";
  } else {
    appStatus.value = "unlocked";
  }
});

function onUnlocked(hex: string) {
  pubkeyHex.value = hex;
  appStatus.value = "unlocked";
}

async function refreshTimeline() {
  await timeline.value?.refresh();
}

const copied = ref(false);

async function copyPubkey() {
  if (!pubkeyHex.value) return;
  try {
    await navigator.clipboard.writeText(pubkeyHex.value);
  } catch {
    // WebView が clipboard API を許可していない環境向けフォールバック
    const ta = document.createElement("textarea");
    ta.value = pubkeyHex.value;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    ta.remove();
  }
  copied.value = true;
  setTimeout(() => {
    copied.value = false;
  }, 1500);
}
</script>
<template>
  <div class="flex flex-col h-full bg-gray-50">
    <div v-if="appStatus === 'loading'" class="flex items-center justify-center h-full text-gray-400">
      起動中…
    </div>

    <AccountSetup v-else-if="appStatus === 'not_setup'" @done="onUnlocked" />

    <AccountUnlock v-else-if="appStatus === 'locked'" @done="onUnlocked" />

    <template v-else>
      <header class="flex items-center justify-between px-5 py-3 bg-white border-b border-gray-200 shrink-0">
        <span class="font-bold text-gray-900">deilephila</span>
        <button
          class="text-xs font-mono cursor-pointer"
          :class="copied ? 'text-green-600' : 'text-gray-400 hover:text-gray-600'"
          :title="copied ? 'コピーしました' : 'クリックで公開鍵をコピー'"
          @click="copyPubkey"
        >
          {{ copied ? "コピーしました" : shortPubkey }}
        </button>
      </header>
      <main class="flex-1 overflow-y-auto w-full max-w-2xl mx-auto px-4 py-4 flex flex-col gap-3">
        <PostComposer @posted="refreshTimeline" />
        <FollowManager @changed="refreshTimeline" />
        <Timeline ref="timeline" />
      </main>
    </template>
  </div>
</template>
