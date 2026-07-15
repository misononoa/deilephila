export interface PostData {
  cid: string;
  author: string;
  text: string;
  timestamp: number;
  edited: boolean;
  deleted: boolean;
  author_display_name: string | null;
}

export interface FollowData {
  pubkey: string;
  since: number;
  display_name: string | null;
}

export interface ForkData {
  author: string;
  layer: "event" | "head";
  seq: number;
  cid_a: string;
  cid_b: string;
  observed_at: number;
}
