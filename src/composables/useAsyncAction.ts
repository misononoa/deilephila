import { ref } from "vue";

export function useAsyncAction() {
  const error = ref("");
  const loading = ref(false);

  async function run(action: () => Promise<void>) {
    error.value = "";
    loading.value = true;
    try {
      await action();
    } catch (e) {
      error.value = String(e);
    } finally {
      loading.value = false;
    }
  }

  return { error, loading, run };
}
