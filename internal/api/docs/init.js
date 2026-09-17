Scalar.createApiReference('#app', {
  theme: 'purple',
  url: '/openapi.yaml',
  servers: [{ url: window.location.origin, description: 'This service instance' }],
  authentication: { preferredSecurityScheme: 'machineToken' },
  persistAuth: false,
  proxyUrl: '',
  withDefaultFonts: false,
  telemetry: false,
  agent: { disabled: true },
  mcp: { disabled: true },
  hideClientButton: true,
  showDeveloperTools: 'never',
});
