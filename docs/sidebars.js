// @ts-check
/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Start Here',
      collapsed: false,
      items: [
        'start-here/what-is-noxmqtt',
        'start-here/quickstart',
        'start-here/connect-to-broker',
        'start-here/configure-certificates',
        'start-here/esp-idf-component',
        'start-here/port-to-platform',
      ],
    },
    'intro',
    'mqtt-versions',
    'getting-started',
    'architecture',
    'secure-connections',
    'esp-idf',
    'configuration-guide',
    'porting-guide',
    'public-api',
    'security',
    'security-reporting',
    'documentation-parity-matrix',
    'release-notes',
    'project',
  ],
};

export default sidebars;
