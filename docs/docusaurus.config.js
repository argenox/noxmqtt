// @ts-check
// NoxMQTT documentation site

import {themes as prismThemes} from 'prism-react-renderer';
import {
  DEFAULT_DESCRIPTION,
  DEFAULT_KEYWORDS,
  DEFAULT_TITLE,
  OG_IMAGE,
  SITE_NAME,
  SITE_URL,
} from './seo-defaults.js';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: DEFAULT_TITLE,
  tagline: 'Embedded MQTT 3.1.1 and MQTT 5 client library in C',
  favicon: 'img/noxtls-logo-25.webp',

  future: {
    v4: true,
  },

  url: SITE_URL,
  baseUrl: '/',

  onBrokenLinks: 'warn',
  onBrokenAnchors: 'warn',
  trailingSlash: false,

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.js',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      },
    ],
  ],

  themeConfig: {
    image: OG_IMAGE,
    metadata: [
      {name: 'description', content: DEFAULT_DESCRIPTION},
      {name: 'keywords', content: DEFAULT_KEYWORDS},
      {name: 'author', content: 'Argenox Technologies LLC'},
      {name: 'robots', content: 'index, follow'},
      {property: 'og:type', content: 'website'},
      {property: 'og:site_name', content: SITE_NAME},
      {property: 'og:locale', content: 'en_US'},
      {name: 'twitter:card', content: 'summary_large_image'},
    ],
    navbar: {
      title: 'NoxMQTT',
      logo: {
        alt: 'NoxMQTT',
        src: 'img/noxtls-logo-25.webp',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          href: 'https://github.com/argenox/noxmqtt',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Introduction', to: '/docs/intro'},
            {label: 'Quickstart', to: '/docs/start-here/quickstart'},
            {label: 'Architecture', to: '/docs/architecture'},
            {label: 'Secure Connections', to: '/docs/secure-connections'},
            {label: 'ESP-IDF', to: '/docs/esp-idf'},
            {label: 'Public API', to: '/docs/public-api'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'Feature Matrix', to: '/docs/documentation-parity-matrix'},
            {label: 'Release Notes', to: '/docs/release-notes'},
            {label: 'Project Roadmap', to: '/docs/project'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/argenox/noxmqtt'},
            {label: 'Contact', href: 'mailto:info@argenox.com'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Argenox Technologies LLC.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['c', 'cmake', 'bash', 'json'],
    },
  },
};

export default config;
