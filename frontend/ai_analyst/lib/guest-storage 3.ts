// for guests (unauthenticated users), we store conversation history in sessionStorage, 
// which is scoped to a single browser tab and cleared when the tab is closed. 
// This allows guests to have a persistent conversation history without needing an account, while ensuring privacy and isolation between users.

const GUEST_CONVERSATIONS_KEY = "guest_conversations"
const GUEST_INDEX_KEY = "guest_conversation_index"

export type GuestMessage = { role: "user" | "assistant"; content: string }

export type GuestConversationMeta = {
  conversationId: string
  latestMessageAt: number
  title: string
}

function readIndex(): GuestConversationMeta[] {
  try {
    const raw = sessionStorage.getItem(GUEST_INDEX_KEY)
    return raw ? (JSON.parse(raw) as GuestConversationMeta[]) : []
  } catch {
    return []
  }
}

function writeIndex(index: GuestConversationMeta[]) {
  sessionStorage.setItem(GUEST_INDEX_KEY, JSON.stringify(index))
}

export const guestStorage = {
  // returns either an mepty array if no history, or an array of messages if there is history for the conversationId
  getConversation(conversationId: string): GuestMessage[] {
    try {
      const raw = sessionStorage.getItem(`${GUEST_CONVERSATIONS_KEY}:${conversationId}`)
      return raw ? (JSON.parse(raw) as GuestMessage[]) : []
    } catch {
      return []
    }
  },

  // saves the conversation messages and updates the conversation index with a title and timestamp
  saveConversation(conversationId: string, messages: GuestMessage[]) {
    try {
      sessionStorage.setItem(
        `${GUEST_CONVERSATIONS_KEY}:${conversationId}`,
        JSON.stringify(messages)
      )

      // Derive title from first user message
      const firstUser = messages.find((m) => m.role === "user")
      const title = firstUser ? firstUser.content.slice(0, 60) : "New chat"

      const index = readIndex()
      const existing = index.findIndex((c) => c.conversationId === conversationId)
      const meta: GuestConversationMeta = {
        conversationId,
        latestMessageAt: Date.now(),
        title,
      }
      if (existing >= 0) {
        index[existing] = meta
      } else {
        index.unshift(meta)
      }
      writeIndex(index)
    } catch {
      // sessionStorage full or unavailable — fail silently
    }
  },

  // returns a list of all conversations sorted newest-first
  listConversations(): GuestConversationMeta[] {
    return readIndex().sort((a, b) => b.latestMessageAt - a.latestMessageAt)
  },

  // delete a conversation from sessionStorage
  deleteConversation(conversationId: string) {
    try {
      sessionStorage.removeItem(`${GUEST_CONVERSATIONS_KEY}:${conversationId}`)
      const index = readIndex().filter((c) => c.conversationId !== conversationId)
      writeIndex(index)
    } catch {
      // ignore
    }
  },
}