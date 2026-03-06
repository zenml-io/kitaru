import { Inter } from 'next/font/google';
import { Provider } from '@/components/provider';
import type { Metadata } from 'next';
import './global.css';

const inter = Inter({
  subsets: ['latin'],
});

export const metadata: Metadata = {
  metadataBase: new URL('https://kitaru.ai'),
  title: {
    default: 'Kitaru Documentation',
    template: '%s | Kitaru',
  },
  description:
    'Durable execution for AI agents. Primitives that make agent workflows persistent, replayable, and observable.',
  icons: {
    icon: '/favicon.svg',
  },
  openGraph: {
    siteName: 'Kitaru Documentation',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
  },
};

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" className={inter.className} suppressHydrationWarning>
      <head>
        <script
          async
          src="https://plausible.io/js/pa-Hii8j2wmtBMkcWoTIEWm6.js"
        />
        <script
          dangerouslySetInnerHTML={{
            __html:
              'window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)};plausible.init=plausible.init||function(i){plausible.o=i||{}};plausible.init()',
          }}
        />
      </head>
      <body className="flex flex-col min-h-screen">
        <Provider>{children}</Provider>
      </body>
    </html>
  );
}
