import type React from "react"
import type { Metadata } from "next"
import { Analytics } from "@vercel/analytics/next"
import "./globals.css"
import { SidebarProvider, SidebarInset, SidebarTrigger } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { ThemeProvider } from "next-themes"
import { ThemeToggle } from "@/components/theme-toggle"
import { AuthProvider } from "@/context/auth-context"

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
              <SidebarInset className="bg-[#c7cad1] text-zinc-900 dark:bg-[#1c1d21] dark:text-zinc-100">
                <header className="sticky top-0 z-30 flex h-16 shrink-0 items-center gap-2 border-b border-zinc-500 bg-[#bcc1c9] px-4 dark:border-zinc-800 dark:bg-[#16171b]">
                  <SidebarTrigger />
                  <div className="flex items-center gap-2">
                    <h1 className="text-xl font-medium tracking-wide text-red-400">HoopQuery</h1>
                  </div>
                  <div className="ml-auto">
                    <ThemeToggle />
                  </div>
                </header>
                <main className="flex-1 overflow-auto bg-[#c7cad1] p-4 dark:bg-[#1c1d21]">{children}</main>
              </SidebarInset>
            </SidebarProvider>
          </AuthProvider>
        </ThemeProvider>
        <Analytics />
      </body>
    </html>
  )
}