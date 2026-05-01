"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { v4 as uuidv4 } from "uuid"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Send } from "lucide-react"

export default function Home() {
  const router = useRouter()
  const [message, setMessage] = useState("")

  useEffect(() => {
    const resetHome = () => setMessage("")
    window.addEventListener("analyst-new-chat", resetHome)
    return () => window.removeEventListener("analyst-new-chat", resetHome)
  }, [])

  const startChat = (question?: string) => {
    const text = (question ?? message).trim()
    if (!text) return
    const newId = uuidv4()
    sessionStorage.setItem(`prefill:${newId}`, text)
    router.push(`/chat/${newId}`)
  }

  return (
    <div className="mx-auto flex min-h-0 w-full max-w-4xl flex-1 flex-col">
      <div className="flex min-h-0 flex-1 flex-col items-center justify-center overflow-y-auto px-4 text-center">
        <h1 className="text-4xl font-bold mb-4">Basketball Analyst</h1>
        <p className="text-muted-foreground text-lg mb-8">
          Ask me anything about basketball stats, players, or teams
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full max-w-2xl">
          {[
            "Compare LeBron James and Stephen Curry's career stats",
            "Show me Steph CUryy's 3 point percentage over his career",
            "What are the Lakers' win-loss records in 2022?",
            "Analyze Giannis Antetokounmpo's playoff performance in 2023",
          ].map((example) => (
            <div
              key={example}
              className="cursor-pointer rounded-lg border-0 bg-[var(--surface-matte-raised)] p-4 text-zinc-900 transition-colors hover:bg-[var(--surface-matte-hover)] dark:bg-[var(--surface-matte-raised)] dark:text-zinc-100 dark:hover:bg-[var(--surface-matte-hover)]"
              onClick={() => setMessage(example)}
            >
              <p className="text-sm">{example}</p>
            </div>
          ))}
        </div>
      </div>

      <div className="shrink-0 border-t border-[var(--surface-matte-border)] bg-[var(--surface-matte)] p-4 dark:border-[var(--surface-matte-border)] dark:bg-[var(--surface-matte)]">
        <div className="mx-auto flex max-w-4xl gap-2">
          <Textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault()
                startChat()
              }
            }}
            placeholder="Ask about basketball stats..."
            className="min-h-[60px] resize-none border-[var(--surface-matte-border)] bg-[var(--surface-matte)] text-zinc-900 shadow-none placeholder:text-zinc-600 dark:border-[var(--surface-matte-border)] dark:bg-[var(--surface-matte)] dark:text-zinc-100 dark:placeholder:text-zinc-400"
          />
          <Button
            onClick={() => startChat()}
            size="icon"
            className="h-[60px] w-[60px] border border-red-400 bg-red-500 text-zinc-100 hover:bg-red-400"
            disabled={!message.trim()}
          >
            <Send className="h-5 w-5 text-zinc-600" />
          </Button>
        </div>
      </div>
    </div>
  )
}