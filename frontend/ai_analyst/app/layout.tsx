import type React from "react"
import type { Metadata } from "next"
import { Analytics } from "@vercel/analytics/next"
import "./globals.css"
import { SidebarProvider, SidebarInset, SidebarTrigger } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { ThemeProvider } from "next-themes"
import { ThemeToggle } from "@/components/theme-toggle"
import { AuthProvider } from "@/context/auth-context"
import { BasketballDecorBackground } from "@/components/basketball-decor-background"

export const metadata: Metadata = {
  title: "v0 App",
  description: "Created with v0",
  generator: "v0.app",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en">
      <body className={`font-sans antialiased`}>
        <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
          <AuthProvider>
            <SidebarProvider>
              <AppSidebar />
              <SidebarInset className="min-h-svh bg-[var(--surface-matte)] text-zinc-900 dark:bg-[var(--surface-matte)] dark:text-zinc-100">
                <header className="sticky top-0 z-30 flex h-16 min-w-0 shrink-0 items-center gap-2 border-b border-[var(--surface-matte-border)] bg-[var(--surface-matte-raised)] px-4 dark:border-[var(--surface-matte-border)] dark:bg-[var(--surface-matte-raised)]">
                  <SidebarTrigger />
                  <div className="flex items-center gap-2">
                    <h1 className="text-xl font-medium tracking-wide text-red-800">HoopQuery</h1>
                  </div>
                  <div className="ml-auto">
                    <ThemeToggle />
                  </div>
                </header>
                <main className="relative flex min-h-0 min-w-0 flex-1 flex-col overflow-x-hidden overflow-y-auto bg-[var(--surface-matte)] p-4 dark:bg-[var(--surface-matte)]">
                  <BasketballDecorBackground />
                  <div className="relative z-10 flex min-h-0 min-w-0 w-full flex-1 flex-col">{children}</div>
                </main>
              </SidebarInset>
            </SidebarProvider>
          </AuthProvider>
        </ThemeProvider>
        <Analytics />
      </body>
    </html>
  )
}
