const STORAGE_KEY = "harbor-command-state";
const LOCAL_APP_ORIGIN = "http://127.0.0.1:8787";
const LOCALHOST_APP_ORIGIN = "http://localhost:8787";
const LOOPBACK_HOSTNAME_PATTERN = /^(?:localhost|127(?:\.\d{1,3}){3}|::1)$/u;
const API_BASE = window.location.protocol === "file:" ? `${LOCAL_APP_ORIGIN}/api` : "/api";
const API_SYNC_DELAY_MS = 350;
const CHARTERS_TAB_ENABLED = false;
const VIEW_OPTIONS = ["overview", "vessel", "maintenance", "work-orders", "charters", "crew", "reports", "vendors", "inventory", "expenses", "voyage"];
const MAINTENANCE_FILTERS = ["all", "due-soon", "completed", "overdue"];
const MAINTENANCE_WORKSPACE_VIEWS = ["overview", "tasks", "systems", "new-service-task", "history"];
const WORK_ORDER_SORT_OPTIONS = ["date-asc", "date-desc", "task-asc", "task-desc", "status"];
const MAINTENANCE_SORT_OPTIONS = ["category", "due-date-asc", "due-date-desc", "priority", "task-asc"];
const VENDOR_SORT_OPTIONS = ["name-asc", "name-desc", "category", "status"];
const INVENTORY_SORT_OPTIONS = ["name-asc", "name-desc", "quantity-low", "quantity-high", "location"];
const EXPENSE_SORT_OPTIONS = ["date-desc", "date-asc", "amount-desc", "amount-asc", "vendor"];
const CHARTER_SORT_OPTIONS = ["start-asc", "start-desc", "client-asc", "status"];
const VOYAGE_SORT_OPTIONS = ["departure-asc", "departure-desc", "route-asc", "status"];
const WEATHER_REFRESH_MS = 30 * 60 * 1000;
const USER_ADMIN_ROLES = ["Captain", "Management"];
const VESSEL_COLLECTION_KEYS = ["maintenanceAssets", "maintenance", "maintenanceHistory", "workOrders", "inventory", "expenses", "charters", "crew", "reports", "vendors", "voyages"];
const MAINTENANCE_CATEGORY_ORDER = [
  "Engines",
  "Generator",
  "AC / Chiller System",
  "Bilge / Pump System",
  "Batteries & Electrical",
  "Fuel System",
  "Fresh Water System",
  "Heads / Sanitation System",
  "Exterior Washdown / General Deck",
  "Safety Equipment",
  "Water System",
  "Tender / Jet Ski",
  "General",
];
const VENDOR_FILTERS = ["all", "active", "under-review"];
const DEFAULT_VESSEL_NAME = "LTD";
const DEFAULT_VESSEL_RUNTIME_ID = "vessel-default";
const LEGACY_VESSEL_NAME = "Asteria";
const DEFAULT_MAINTENANCE_INTERVAL_DAYS = 7;
const DEFAULT_MAINTENANCE_REMINDER_DAYS = 3;
const DEFAULT_MAINTENANCE_INTERVAL_HOURS = 0;
const DEFAULT_MAINTENANCE_REMINDER_HOURS = 0;
const EXPENSE_CHART_COLORS = ["#f6c56e", "#4cb7a5", "#6fb7ff", "#ef7c62", "#b78aff", "#7ed7bf"];
const AUTH_SERVICE_UNAVAILABLE_MESSAGE = "Unable to reach the Harbor Command authentication service.";
const AUTH_SERVICE_RECOVERY_DELAY_MS = 5000;
const APP_ROOT_URL = window.location.protocol === "file:"
  ? `${LOCAL_APP_ORIGIN}/`
  : LOOPBACK_HOSTNAME_PATTERN.test(window.location.hostname) && window.location.port === "8787"
  ? `${LOCAL_APP_ORIGIN}/`
  : `${window.location.origin}/`;
let resolvedApiBase = API_BASE;

const defaultState = {
  vessel: {
    name: DEFAULT_VESSEL_NAME,
    builder: "Lazzara",
    model: "Sanlorenzo SX88",
    yearBuilt: 2022,
    vesselType: "Crossover Yacht",
    hullMaterial: "GRP",
    catalogManufacturerId: "",
    catalogModelId: "",
    catalogSpecId: "",
    isCustom: true,
    length: 88,
    beam: 23,
    draft: 5.8,
    guests: 12,
    status: "Ready",
    berth: "Nassau Marina B2",
    captain: "Elena Rossi",
    location: "Nassau",
    fuel: 84,
    fuelCapacity: 5200,
    waterTank: 72,
    waterCapacity: 950,
    greyTank: 26,
    greyWaterCapacity: 420,
    blackTankLevel: 18,
    blackWaterCapacity: 320,
    batteryStatus: 91,
    utilization: 71,
    nextService: "2026-04-18",
    engineInfo: "Twin MAN V12 1550HP | 1,248 hrs",
    generatorInfo: "Twin Kohler 28kW | Port 812 hrs | Starboard 796 hrs",
    engines: [
      {
        id: "engine-port",
        label: "Port Main",
        manufacturer: "MAN",
        model: "V12 1550",
        rating: "1550 HP",
        hours: 1248,
        lastServiceHours: 1000,
        serviceIntervalHours: 250,
        lastServiceDate: "2026-04-02",
        nextServiceDate: "2026-05-18",
        notes: "Oil service, filters, and hose inspection completed.",
      },
      {
        id: "engine-starboard",
        label: "Starboard Main",
        manufacturer: "MAN",
        model: "V12 1550",
        rating: "1550 HP",
        hours: 1241,
        lastServiceHours: 1000,
        serviceIntervalHours: 250,
        lastServiceDate: "2026-04-02",
        nextServiceDate: "2026-05-18",
        notes: "Valve review clean. Heat exchanger check logged.",
      },
    ],
    generators: [
      {
        id: "generator-port",
        label: "Port Generator",
        manufacturer: "Kohler",
        model: "28kW",
        rating: "28 kW",
        hours: 812,
        lastServiceHours: 650,
        serviceIntervalHours: 200,
        lastServiceDate: "2026-04-07",
        nextServiceDate: "2026-05-07",
        notes: "Raw water impeller and exhaust inspection tracked.",
      },
      {
        id: "generator-starboard",
        label: "Starboard Generator",
        manufacturer: "Kohler",
        model: "28kW",
        rating: "28 kW",
        hours: 796,
        lastServiceHours: 650,
        serviceIntervalHours: 200,
        lastServiceDate: "2026-04-07",
        nextServiceDate: "2026-05-07",
        notes: "Fuel filter and oil service due in next interval window.",
      },
    ],
    photoDataUrl: "",
    notes:
      "Prepared for owner arrival with water toys inventoried, interior refreshed, and provisioning confirmed.",
  },
  maintenanceAssets: [],
  maintenance: [],
  maintenanceHistory: [],
  workOrders: [],
  charters: [
    {
      id: "charter-1",
      client: "Owner weekend",
      start: "2026-04-20",
      end: "2026-04-23",
      berth: "Highbourne Cay",
      status: "Confirmed",
    },
  ],
  crew: [
    {
      id: "crew-1",
      name: "Elena Rossi",
      role: "Captain",
      certification: "Master 500GT",
      rotation: "On board",
    },
    {
      id: "crew-2",
      name: "Nia Porter",
      role: "Chief stewardess",
      certification: "Medical care",
      rotation: "On board",
    },
    {
      id: "crew-3",
      name: "Rafael Quinn",
      role: "Bosun",
      certification: "Tender operator",
      rotation: "On board",
    },
  ],
  activeReportId: "weekly-report-1",
  reports: [
    {
      id: "weekly-report-1",
      weekStart: "2026-04-13",
      weekEnd: "2026-04-17",
      status: "draft",
      createdAt: "2026-04-13T08:00:00.000Z",
      updatedAt: "2026-04-16T08:00:00.000Z",
      entries: [
        {
          id: "report-entry-1",
          item: "Laundry",
          reportDate: "2026-04-14",
          workDone: "Took to wash",
          systemsChecked: "",
          issues: "",
          notes: "",
        },
        {
          id: "report-entry-2",
          item: "Galley Storage",
          reportDate: "2026-04-14",
          workDone: "Removed screws, alignment and re-installed",
          systemsChecked: "Storage hatch alignment",
          issues: "Storage hatch not fully closing",
          notes: "Follow up on hinge tolerance before owner turnover.",
        },
        {
          id: "report-entry-3",
          item: "Fuel Racor",
          reportDate: "2026-04-13",
          workDone: "Weekly check",
          systemsChecked: "Fuel filtration",
          issues: "",
          notes: "",
        },
      ],
    },
  ],
  vendors: [
    {
      id: "vendor-1",
      name: "ALEX A/C",
      contact: "ALEX",
      email: "N/A",
      phone: "786-296-5200",
      status: "Active",
      category: "A/C Tech",
    },
    {
      id: "vendor-2",
      name: "FREDDIE FRIDGE",
      contact: "FREDDIE",
      email: "N/A",
      phone: "786-835-6075",
      status: "Under review",
      category: "Appliance Repair",
    },
    {
      id: "vendor-3",
      name: "JOFRAN FRIDGE",
      contact: "JOFRAN",
      email: "N/A",
      phone: "786-675-1860",
      status: "Under review",
      category: "Appliance Repair",
    },
    {
      id: "vendor-4",
      name: "Sub-Zero Tech",
      contact: "Hornand",
      email: "N/A",
      phone: "786-343-4795",
      status: "Active",
      category: "Appliance Repair",
    },
    {
      id: "vendor-5",
      name: "A/C Chillers Distributor",
      contact: "Federico",
      email: "N/A",
      phone: "786-354-0716",
      status: "Under review",
      category: "Appliance Repair",
    },
    {
      id: "vendor-6",
      name: "WAX CLEAN COMPANY",
      contact: "JORDAN",
      email: "N/A",
      phone: "954-662-4069",
      status: "Active",
      category: "Wax Polish",
    },
    {
      id: "vendor-7",
      name: "JOSE LAZZARA ELECTRICIAN",
      contact: "JOSE",
      email: "N/A",
      phone: "305-546-5230",
      status: "Under review",
      category: "Electrician",
    },
    {
      id: "vendor-8",
      name: "CAPTAIN",
      contact: "Andre Sayegh",
      email: "N/A",
      phone: "786-740-2582",
      status: "Active",
      category: "Captain",
    },
    {
      id: "vendor-9",
      name: "INTREPID TECH",
      contact: "Roberto",
      email: "N/A",
      phone: "786-710-0035",
      status: "Active",
      category: "Intrepid Tech",
    },
    {
      id: "vendor-10",
      name: "Tetris Commercial Diving",
      contact: "300$ Entire Vessel",
      email: "N/A",
      phone: "786-641-3172",
      status: "Active",
      category: "Diver",
    },
  ],
  inventory: [],
  expenses: [],
  voyages: [
    {
      id: "voyage-1",
      route: "Nassau to Highbourne Cay",
      departure: "2026-04-20",
      weather: "Moderate swell, clear visibility",
      status: "Standby",
    },
    {
      id: "voyage-2",
      route: "Harbor departure rehearsal",
      departure: "2026-04-17",
      weather: "Light easterly breeze",
      status: "Scheduled",
    },
  ],
};

const state = loadState();
let editingMaintenanceAssetId = null;
let editingMaintenanceId = null;
let editingWorkOrderId = null;
let editingVendorId = null;
let editingInventoryId = null;
let editingExpenseId = null;
let editingEngineId = null;
let editingGeneratorId = null;
let weatherState = createDefaultWeatherState();
const vesselCatalogState = {
  years: [],
  manufacturers: [],
  models: [],
  specs: [],
  engineManufacturers: [],
  generatorManufacturers: [],
  maintenanceTemplates: [],
  loaded: false,
  loading: false,
  error: "",
};
const addVesselState = {
  customName: "",
  year: "",
  manufacturerQuery: "",
  manufacturerId: "",
  modelQuery: "",
  modelId: "",
  specId: "",
  customManufacturer: "",
  customModel: "",
  photoDataUrl: "",
};
let apiSyncTimer = null;
let apiPersistInFlight = false;
let apiPersistQueued = false;
const vesselAddUiState = {
  message: "",
  error: "",
};
let apiAvailable = false;
let apiWarningShown = false;
let pendingStateSnapshot = null;
const managedInvitesState = {
  items: [],
  loading: false,
  error: "",
  notice: "",
  latestInvite: null,
  delivery: {
    ready: false,
    provider: "Resend",
    fromEmail: "",
    publicAppUrl: "",
    message: "",
  },
};
const managedUsersState = {
  items: [],
  availableVessels: [],
  loading: false,
  error: "",
  notice: "",
};
const inviteState = {
  token: "",
  loading: false,
  pending: false,
  invite: null,
  error: "",
};
const authState = {
  authenticated: false,
  hasUsers: false,
  mode: "loading",
  pending: true,
  user: null,
  error: "",
};
const UI_SCROLL_STORAGE_KEY = `${STORAGE_KEY}:ui-scroll`;
let pendingUiScrollState = readPendingUiScrollState();
let authRecoveryTimer = null;
let authRecoveryInFlight = false;

if ("scrollRestoration" in window.history) {
  window.history.scrollRestoration = "manual";
}

window.addEventListener("pagehide", handlePageExit);
window.addEventListener("beforeunload", handlePageExit);
window.addEventListener("online", triggerAuthRecoveryCheck);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    triggerAuthRecoveryCheck();
  }
});

const elements = {
  bootSplash: document.querySelector("#boot-splash"),
  bootSplashCopy: document.querySelector("#boot-splash-copy"),
  authShell: document.querySelector("#auth-shell"),
  authEyebrow: document.querySelector("#auth-eyebrow"),
  authTitle: document.querySelector("#auth-title"),
  authCopy: document.querySelector("#auth-copy"),
  authError: document.querySelector("#auth-error"),
  authServiceHelp: document.querySelector("#auth-service-help"),
  authOpenLocal: document.querySelector("#auth-open-local"),
  authRetry: document.querySelector("#auth-retry"),
  authForm: document.querySelector("#auth-form"),
  authNameRow: document.querySelector("#auth-name-row"),
  authNameInput: document.querySelector("#auth-name-input"),
  authEmailRow: document.querySelector("#auth-email-row"),
  authEmailInput: document.querySelector("#auth-email-input"),
  authRoleRow: document.querySelector("#auth-role-row"),
  authRoleSelect: document.querySelector("#auth-role-select"),
  authPasswordRow: document.querySelector("#auth-password-row"),
  authPasswordLabel: document.querySelector("#auth-password-label"),
  authPasswordInput: document.querySelector("#auth-password-input"),
  authSubmit: document.querySelector("#auth-submit"),
  authToggle: document.querySelector("#auth-toggle"),
  accessModal: document.querySelector("#access-modal"),
  accessModalBackdrop: document.querySelector("#access-modal-backdrop"),
  openAccessModal: document.querySelector("#open-access-modal"),
  closeAccessModal: document.querySelector("#close-access-modal"),
  inviteForm: document.querySelector("#invite-form"),
  inviteSubmit: document.querySelector("#invite-submit"),
  inviteVesselPicker: document.querySelector("#invite-vessel-picker"),
  inviteVesselHelp: document.querySelector("#invite-vessel-help"),
  inviteFeedback: document.querySelector("#invite-feedback"),
  invitePreview: document.querySelector("#invite-preview"),
  inviteList: document.querySelector("#invite-list"),
  summaryCards: document.querySelector("#summary-cards"),
  vesselDashboard: document.querySelector("#vessel-dashboard"),
  spotlightPanel: document.querySelector("#spotlight-panel"),
  maintenanceAssetsList: document.querySelector("#maintenance-assets-list"),
  maintenanceAssetForm: document.querySelector("#maintenance-asset-form"),
  maintenanceAssetSubmit: document.querySelector("#maintenance-asset-submit"),
  maintenanceAssetCancel: document.querySelector("#maintenance-asset-cancel"),
  maintenanceAssetTemplatePreview: document.querySelector("#maintenance-asset-template-preview"),
  maintenanceHistoryList: document.querySelector("#maintenance-history-list"),
  maintenanceQuickActions: document.querySelector("#maintenance-quick-actions"),
  maintenanceSubnav: document.querySelector("#maintenance-subnav"),
  maintenanceSubtabs: Array.from(document.querySelectorAll("[data-maintenance-view]")),
  maintenanceWorkspacePanels: Array.from(document.querySelectorAll("[data-maintenance-panel]")),
  jumpToMaintenanceAssetForm: document.querySelector("#jump-to-maintenance-asset-form"),
  maintenanceApplyStarterPack: document.querySelector("#maintenance-apply-starter-pack"),
  maintenanceAdvanced: document.querySelector("#maintenance-advanced"),
  overviewPanel: document.querySelector("#overview-panel"),
  maintenanceList: document.querySelector("#maintenance-list"),
  maintenanceFilters: document.querySelector("#maintenance-filters"),
  maintenanceSearch: document.querySelector("#maintenance-search"),
  maintenanceSort: document.querySelector("#maintenance-sort"),
  maintenanceFocus: document.querySelector("#maintenance-focus"),
  maintenanceOverviewSpotlight: document.querySelector("#maintenance-overview-spotlight"),
  maintenanceSections: document.querySelector("#maintenance-sections"),
  maintenanceSectionSummary: document.querySelector("#maintenance-section-summary"),
  workOrderList: document.querySelector("#work-order-list"),
  workOrderTitle: document.querySelector("#work-order-title"),
  workOrderPeriod: document.querySelector("#work-order-period"),
  workOrderSort: document.querySelector("#work-order-sort"),
  workOrderGenerateReport: document.querySelector("#work-order-generate-report"),
  workOrderOpenReports: document.querySelector("#work-order-open-reports"),
  workOrderActionNote: document.querySelector("#work-order-action-note"),
  workOrderSummaryCards: document.querySelector("#work-order-summary-cards"),
  workOrderListCopy: document.querySelector("#work-order-list-copy"),
  workOrderFormContext: document.querySelector("#work-order-form-context"),
  charterList: document.querySelector("#charter-list"),
  charterSort: document.querySelector("#charter-sort"),
  crewList: document.querySelector("#crew-list"),
  usersList: document.querySelector("#users-list"),
  usersFeedback: document.querySelector("#users-feedback"),
  reportTitle: document.querySelector("#report-title"),
  reportPeriod: document.querySelector("#report-period"),
  reportHistorySelect: document.querySelector("#report-history-select"),
  reportViewButton: document.querySelector("#report-view-button"),
  reportExportPdf: document.querySelector("#report-export-pdf"),
  reportToggleStatus: document.querySelector("#report-toggle-status"),
  reportActionNote: document.querySelector("#report-action-note"),
  reportStatusBadge: document.querySelector("#report-status-badge"),
  reportSummaryCards: document.querySelector("#report-summary-cards"),
  reportPreviewList: document.querySelector("#report-preview-list"),
  reportPreviewCopy: document.querySelector("#report-preview-copy"),
  reportPreviewSection: document.querySelector("#report-preview-section"),
  vendorFilters: document.querySelector("#vendor-filters"),
  vendorSort: document.querySelector("#vendor-sort"),
  vendorTable: document.querySelector("#vendor-table"),
  inventoryList: document.querySelector("#inventory-list"),
  inventorySort: document.querySelector("#inventory-sort"),
  expensesAnalytics: document.querySelector("#expenses-analytics"),
  expenseSort: document.querySelector("#expense-sort"),
  expensesList: document.querySelector("#expenses-list"),
  voyageSort: document.querySelector("#voyage-sort"),
  voyageList: document.querySelector("#voyage-list"),
  alertFeed: document.querySelector("#alert-feed"),
  workspaceNav: document.querySelector("#workspace-nav"),
  workspaceTabs: Array.from(document.querySelectorAll("[data-view]")),
  viewPanels: Array.from(document.querySelectorAll("[data-view-panel]")),
  vesselAddForm: document.querySelector("#vessel-add-form"),
  vesselCatalogPreview: document.querySelector("#vessel-catalog-preview"),
  vesselManufacturerOptions: document.querySelector("#vessel-manufacturer-options"),
  vesselModelOptions: document.querySelector("#vessel-model-options"),
  vesselAddPhotoInput: document.querySelector("#vessel-add-photo-input"),
  clearVesselAddPhoto: document.querySelector("#clear-vessel-add-photo"),
  vesselAddFeedback: document.querySelector("#vessel-add-feedback"),
  createCustomVesselButton: document.querySelector("#create-custom-vessel"),
  vesselForm: document.querySelector("#vessel-form"),
  vesselSystemsForm: document.querySelector("#vessel-systems-form"),
  vesselPhotoInput: document.querySelector("#vessel-photo-input"),
  clearVesselPhoto: document.querySelector("#clear-vessel-photo"),
  engineList: document.querySelector("#engine-list"),
  engineForm: document.querySelector("#engine-form"),
  engineManufacturerOptions: document.querySelector("#engine-manufacturer-options"),
  engineSubmit: document.querySelector("#engine-submit"),
  engineCancel: document.querySelector("#engine-cancel"),
  generatorList: document.querySelector("#generator-list"),
  generatorForm: document.querySelector("#generator-form"),
  generatorManufacturerOptions: document.querySelector("#generator-manufacturer-options"),
  generatorSubmit: document.querySelector("#generator-submit"),
  generatorCancel: document.querySelector("#generator-cancel"),
  maintenanceForm: document.querySelector("#maintenance-form"),
  maintenanceSubmit: document.querySelector("#maintenance-submit"),
  maintenanceCancel: document.querySelector("#maintenance-cancel"),
  workOrderForm: document.querySelector("#work-order-form"),
  workOrderSubmit: document.querySelector("#work-order-submit"),
  workOrderCancel: document.querySelector("#work-order-cancel"),
  charterForm: document.querySelector("#charter-form"),
  vendorForm: document.querySelector("#vendor-form"),
  vendorSubmit: document.querySelector("#vendor-submit"),
  vendorCancel: document.querySelector("#vendor-cancel"),
  inventoryForm: document.querySelector("#inventory-form"),
  inventorySubmit: document.querySelector("#inventory-submit"),
  inventoryCancel: document.querySelector("#inventory-cancel"),
  expenseForm: document.querySelector("#expense-form"),
  expenseSubmit: document.querySelector("#expense-submit"),
  expenseCancel: document.querySelector("#expense-cancel"),
  jumpToVessel: document.querySelector("#jump-to-vessel"),
  jumpToMaintenanceForm: document.querySelector("#jump-to-maintenance-form"),
  jumpToWorkOrderForm: document.querySelector("#jump-to-work-order-form"),
  jumpToVendorForm: document.querySelector("#jump-to-vendor-form"),
  jumpToInventoryForm: document.querySelector("#jump-to-inventory-form"),
  jumpToExpenseForm: document.querySelector("#jump-to-expense-form"),
  logoutButton: document.querySelector("#logout-button"),
};

try {
  hydrateState();
  bindEvents();
  renderAuthShell();
  void bootstrapAuth();
} catch (error) {
  console.error("Harbor Command failed to initialize.", error);
  const bootSplashElement = document.querySelector("#boot-splash");
  const authShellElement = document.querySelector("#auth-shell");
  const authErrorElement = document.querySelector("#auth-error");
  const authTitleElement = document.querySelector("#auth-title");
  const authCopyElement = document.querySelector("#auth-copy");
  const authServiceHelpElement = document.querySelector("#auth-service-help");

  if (bootSplashElement) {
    bootSplashElement.hidden = true;
  }

  if (authShellElement) {
    authShellElement.hidden = false;
  }

  document.body.classList.remove("booting");

  if (authTitleElement) {
    authTitleElement.textContent = "Harbor Command needs attention";
  }

  if (authCopyElement) {
    authCopyElement.textContent = "The app hit a startup issue before the sign-in flow could load completely.";
  }

  if (authErrorElement) {
    authErrorElement.hidden = false;
    authErrorElement.textContent = error instanceof Error
      ? `Startup error: ${error.message}`
      : "Startup error: Harbor Command could not finish loading.";
  }

  if (authServiceHelpElement) {
    authServiceHelpElement.hidden = false;
  }
}

function loadState() {
  const baseState = createBaseState();
  let stored = "";

  try {
    stored = localStorage.getItem(STORAGE_KEY);
  } catch (error) {
    return baseState;
  }

  if (!stored) {
    return baseState;
  }

  try {
    const parsed = JSON.parse(stored);
    return normalizeLoadedState(parsed, baseState);
  } catch (error) {
    return baseState;
  }
}

function createBaseState() {
  const defaultStateSnapshot = cloneDefaultState();
  const seededVessel = normalizeVesselRecord(
    {
      id: DEFAULT_VESSEL_RUNTIME_ID,
      ...defaultStateSnapshot.vessel,
    },
    defaultStateSnapshot.vessel,
    0
  );
  const seededBundle = createSeededVesselBundle(defaultStateSnapshot);

  return activateRuntimeVesselState({
    ...defaultStateSnapshot,
    vessels: [seededVessel],
    activeVesselId: seededVessel.id,
    activeReportId: defaultStateSnapshot.activeReportId || seededBundle.reports[0]?.id || "",
    activeWorkWeekStart: getCurrentWeekRange().start,
    vesselBundles: {
      [seededVessel.id]: seededBundle,
    },
    activeView: "overview",
    activeMaintenanceWorkspace: "overview",
    activeWorkOrderSort: "date-asc",
    activeMaintenanceFilter: "all",
    activeMaintenanceSort: "category",
    activeMaintenanceCategory: getDefaultMaintenanceCategory(defaultStateSnapshot.maintenance),
    activeMaintenanceQuery: "",
    activeVendorFilter: "all",
    activeVendorSort: "name-asc",
    activeInventorySort: "name-asc",
    activeExpenseSort: "date-desc",
    activeCharterSort: "start-asc",
    activeVoyageSort: "departure-asc",
  });
}

function createSeededVesselBundle(sourceState) {
  return {
    maintenanceAssets: cloneItems(sourceState.maintenanceAssets || []),
    maintenance: cloneItems(sourceState.maintenance || []),
    maintenanceHistory: cloneItems(sourceState.maintenanceHistory || []),
    workOrders: cloneItems(sourceState.workOrders || []),
    inventory: cloneItems(sourceState.inventory || []),
    expenses: cloneItems(sourceState.expenses || []),
    charters: cloneItems(sourceState.charters || []),
    crew: cloneItems(sourceState.crew || []),
    reports: cloneItems(sourceState.reports || []),
    vendors: cloneItems(sourceState.vendors || []),
    voyages: cloneItems(sourceState.voyages || []),
  };
}

function createFreshVesselBundle() {
  return {
    maintenanceAssets: [],
    maintenance: [],
    maintenanceHistory: [],
    workOrders: [],
    inventory: [],
    expenses: [],
    charters: [],
    crew: [],
    reports: [],
    vendors: [],
    voyages: [],
  };
}

function buildLegacyTopLevelBundle(sourceState) {
  return {
    maintenanceAssets: sourceState.maintenanceAssets,
    maintenance: sourceState.maintenance,
    maintenanceHistory: sourceState.maintenanceHistory,
    workOrders: sourceState.workOrders,
    inventory: sourceState.inventory,
    expenses: sourceState.expenses,
    charters: sourceState.charters,
    crew: sourceState.crew,
    reports: sourceState.reports,
    vendors: sourceState.vendors,
    voyages: sourceState.voyages,
  };
}

function normalizeVesselRecord(vessel, fallback, index = 0) {
  const vesselFallback = {
    ...fallback,
    name: index === 0 ? fallback.name : `Vessel ${index + 1}`,
  };

  return {
    id: String(vessel?.id || createId("vessel")),
    ...normalizeVesselState(vessel, vesselFallback),
  };
}

function normalizeVesselBundle(bundle, fallbackBundle) {
  const sourceBundle = bundle && typeof bundle === "object" ? bundle : {};
  return {
    maintenanceAssets: normalizeMaintenanceAssetCollection(sourceBundle.maintenanceAssets, fallbackBundle.maintenanceAssets),
    maintenance: normalizeMaintenanceCollection(sourceBundle.maintenance, fallbackBundle.maintenance),
    maintenanceHistory: normalizeMaintenanceHistoryCollection(sourceBundle.maintenanceHistory, fallbackBundle.maintenanceHistory),
    workOrders: normalizeWorkOrderCollection(sourceBundle.workOrders, fallbackBundle.workOrders),
    inventory: normalizeCollection(sourceBundle.inventory, fallbackBundle.inventory),
    expenses: normalizeCollection(sourceBundle.expenses, fallbackBundle.expenses),
    charters: normalizeCollection(sourceBundle.charters, fallbackBundle.charters),
    crew: normalizeCollection(sourceBundle.crew, fallbackBundle.crew),
    reports: normalizeReportCollection(sourceBundle.reports, fallbackBundle.reports),
    vendors: normalizeCollection(sourceBundle.vendors, fallbackBundle.vendors),
    voyages: normalizeCollection(sourceBundle.voyages, fallbackBundle.voyages),
  };
}

function syncActiveRuntimeState(targetState) {
  const nextState = targetState;
  if (!nextState || typeof nextState !== "object") {
    return targetState;
  }

  const activeVesselId = String(nextState.activeVesselId || nextState.vessel?.id || "");
  if (!activeVesselId) {
    return targetState;
  }

  nextState.vessels = Array.isArray(nextState.vessels) ? nextState.vessels : [];
  nextState.vesselBundles = nextState.vesselBundles && typeof nextState.vesselBundles === "object"
    ? nextState.vesselBundles
    : {};

  const activeBundle = nextState.vesselBundles[activeVesselId] || createFreshVesselBundle();
  const syncedBundle = {
    ...activeBundle,
  };

  VESSEL_COLLECTION_KEYS.forEach((key) => {
    syncedBundle[key] = cloneItems(Array.isArray(nextState[key]) ? nextState[key] : []);
    nextState[key] = syncedBundle[key];
  });

  nextState.vesselBundles[activeVesselId] = syncedBundle;

  const currentVessel = nextState.vessel && typeof nextState.vessel === "object"
    ? { ...nextState.vessel, id: activeVesselId }
    : { id: activeVesselId };
  const existingIndex = nextState.vessels.findIndex((vessel) => String(vessel.id) === activeVesselId);
  const syncedVessel = existingIndex >= 0
    ? { ...nextState.vessels[existingIndex], ...currentVessel, id: activeVesselId }
    : currentVessel;

  if (existingIndex >= 0) {
    nextState.vessels[existingIndex] = syncedVessel;
  } else {
    nextState.vessels.push(syncedVessel);
  }

  nextState.vessel = syncedVessel;
  return nextState;
}

function activateRuntimeVesselState(targetState, requestedVesselId) {
  const nextState = syncActiveRuntimeState(targetState);
  const vessels = Array.isArray(nextState.vessels) && nextState.vessels.length
    ? nextState.vessels
    : [normalizeVesselRecord({ id: DEFAULT_VESSEL_RUNTIME_ID, ...nextState.vessel }, defaultState.vessel, 0)];
  const resolvedVesselId = String(
    vessels.some((vessel) => String(vessel.id) === String(requestedVesselId || nextState.activeVesselId))
      ? requestedVesselId || nextState.activeVesselId
      : vessels[0].id
  );

  nextState.vessels = vessels;
  nextState.activeVesselId = resolvedVesselId;
  nextState.vesselBundles = nextState.vesselBundles && typeof nextState.vesselBundles === "object"
    ? nextState.vesselBundles
    : {};

  const activeVessel = vessels.find((vessel) => String(vessel.id) === resolvedVesselId) || vessels[0];
  const activeBundle = nextState.vesselBundles[resolvedVesselId] || createFreshVesselBundle();
  nextState.vesselBundles[resolvedVesselId] = activeBundle;
  nextState.vessel = activeVessel;

  VESSEL_COLLECTION_KEYS.forEach((key) => {
    nextState[key] = activeBundle[key];
  });

  return nextState;
}

function normalizeRuntimeFleetState(parsed, baseState) {
  const normalizedState = parsed && typeof parsed === "object" ? { ...parsed } : {};
  const baseVessel = baseState.vessels?.[0] || normalizeVesselRecord({ id: DEFAULT_VESSEL_RUNTIME_ID, ...baseState.vessel }, baseState.vessel, 0);
  const vesselsSource = Array.isArray(normalizedState.vessels) && normalizedState.vessels.length
    ? normalizedState.vessels
    : [
        {
          id: normalizedState.activeVesselId || normalizedState.vessel?.id || DEFAULT_VESSEL_RUNTIME_ID,
          ...(normalizedState.vessel || baseState.vessel),
        },
      ];
  const vessels = vesselsSource.map((vessel, index) => normalizeVesselRecord(vessel, baseVessel, index));
  const preferredActiveId = String(
    normalizedState.activeVesselId && vessels.some((vessel) => String(vessel.id) === String(normalizedState.activeVesselId))
      ? normalizedState.activeVesselId
      : vessels[0].id
  );
  const incomingBundles = normalizedState.vesselBundles && typeof normalizedState.vesselBundles === "object"
    ? normalizedState.vesselBundles
    : {};
  const legacyBundle = buildLegacyTopLevelBundle(normalizedState);
  const seededFallbackBundle = createSeededVesselBundle(baseState);
  const freshFallbackBundle = createFreshVesselBundle();

  normalizedState.vessels = vessels;
  normalizedState.activeVesselId = preferredActiveId;
  normalizedState.vesselBundles = {};

  vessels.forEach((vessel, index) => {
    const bundleSource = incomingBundles[vessel.id]
      || incomingBundles[String(vessel.id)]
      || (!Object.keys(incomingBundles).length && String(vessel.id) === preferredActiveId ? legacyBundle : null);
    const fallbackBundle = index === 0 && !Object.keys(incomingBundles).length ? seededFallbackBundle : freshFallbackBundle;
    normalizedState.vesselBundles[vessel.id] = normalizeVesselBundle(bundleSource, fallbackBundle);
  });

  return activateRuntimeVesselState(normalizedState, preferredActiveId);
}

function replaceState(nextState) {
  const normalized = normalizeLoadedState(nextState || {}, createBaseState());
  Object.keys(state).forEach((key) => {
    delete state[key];
  });
  Object.assign(state, normalized);
  syncConnectedAutomation(state);
}

function clearCachedState() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch (error) {
    // Ignore storage cleanup failures in restricted browser contexts.
  }
}

function setAuthState(nextValues) {
  Object.assign(authState, nextValues);
}

function setManagedUsersState(nextValues) {
  Object.assign(managedUsersState, nextValues);
}

function setManagedInvitesState(nextValues) {
  Object.assign(managedInvitesState, nextValues);
}

function clearManagedUsersState() {
  setManagedUsersState({
    items: [],
    availableVessels: [],
    loading: false,
    error: "",
    notice: "",
  });
}

function clearManagedInvitesState() {
  setManagedInvitesState({
    items: [],
    loading: false,
    error: "",
    notice: "",
    latestInvite: null,
    delivery: {
      ready: false,
      provider: "Resend",
      fromEmail: "",
      publicAppUrl: "",
      message: "",
    },
  });
}

function setInviteState(nextValues) {
  Object.assign(inviteState, nextValues);
}

function clearInviteState() {
  setInviteState({
    token: "",
    loading: false,
    pending: false,
    invite: null,
    error: "",
  });
}

function normalizeTextValue(value, fallback = "") {
  if (typeof value === "string") {
    return value;
  }

  if (value === null || typeof value === "undefined") {
    return fallback;
  }

  return String(value);
}

function setBootSplashVisible(isVisible, message = "Syncing your vessel workspace.") {
  if (elements.bootSplash) {
    elements.bootSplash.hidden = !isVisible;
  }

  document.body.classList.toggle("booting", isVisible);

  if (isVisible && elements.bootSplashCopy) {
    elements.bootSplashCopy.textContent = message;
  }
}

function clearAuthRecoveryTimer() {
  if (authRecoveryTimer) {
    window.clearTimeout(authRecoveryTimer);
    authRecoveryTimer = null;
  }
}

function isFileModeApp() {
  return window.location.protocol === "file:";
}

function isLoopbackPage() {
  return LOOPBACK_HOSTNAME_PATTERN.test(window.location.hostname) && window.location.port === "8787";
}

function buildApiBaseCandidates() {
  const candidates = [];

  if (API_BASE) {
    candidates.push(API_BASE);
  }

  if (isFileModeApp() || isLoopbackPage()) {
    candidates.push(`${LOCAL_APP_ORIGIN}/api`);
    candidates.push(`${LOCALHOST_APP_ORIGIN}/api`);
  }

  return [...new Set(candidates.filter(Boolean))];
}

function isRetryableHarborFetchError(error) {
  if (error instanceof TypeError) {
    return true;
  }

  const message = error instanceof Error ? error.message : String(error ?? "");
  return /failed to fetch|networkerror|load failed|fetch|network request failed/i.test(message);
}

async function fetchFromHarborApi(path, options = {}) {
  const candidates = [
    resolvedApiBase,
    ...buildApiBaseCandidates().filter((candidate) => candidate !== resolvedApiBase),
  ];
  let lastError = null;

  for (const base of candidates) {
    try {
      const response = await fetch(`${base}${path}`, {
        credentials: "include",
        ...options,
        headers: {
          Accept: "application/json",
          ...(options.headers || {}),
        },
      });
      resolvedApiBase = base;
      return response;
    } catch (error) {
      lastError = error;
      if (!isRetryableHarborFetchError(error)) {
        throw error;
      }
    }
  }

  throw lastError || new Error(AUTH_SERVICE_UNAVAILABLE_MESSAGE);
}

function buildFreshAppUrl(reason = "app") {
  const separator = APP_ROOT_URL.includes("?") ? "&" : "?";
  return `${APP_ROOT_URL}${separator}launch=${encodeURIComponent(reason)}-${Date.now()}`;
}

function isAuthServiceUnavailable(errorMessage = "") {
  return normalizeTextValue(errorMessage, "") === AUTH_SERVICE_UNAVAILABLE_MESSAGE;
}

function getFriendlyAuthError(error, fallbackMessage = "Unable to complete authentication.") {
  if (error instanceof TypeError) {
    return AUTH_SERVICE_UNAVAILABLE_MESSAGE;
  }

  const message = error instanceof Error ? error.message : normalizeTextValue(error, "");
  if (/failed to fetch|networkerror|load failed|fetch/i.test(message)) {
    return AUTH_SERVICE_UNAVAILABLE_MESSAGE;
  }

  return message || fallbackMessage;
}

async function probeAuthService() {
  try {
    const response = await fetchFromHarborApi("/health", {
      cache: "no-store",
    });
    return response.ok;
  } catch (error) {
    return false;
  }
}

async function redirectToLiveHarborCommand() {
  if (!isFileModeApp()) {
    return false;
  }

  const serviceAvailable = await probeAuthService();
  if (!serviceAvailable) {
    return false;
  }

  const redirectUrl = `${buildFreshAppUrl("redirect")}${window.location.hash || ""}`;
  window.location.replace(redirectUrl);
  return true;
}

function redirectToCanonicalLocalOrigin() {
  if (!isLoopbackPage() || window.location.hostname === "127.0.0.1") {
    return false;
  }

  const nextUrl = new URL(window.location.href);
  nextUrl.hostname = "127.0.0.1";
  window.location.replace(nextUrl.toString());
  return true;
}

function scheduleAuthRecovery() {
  if (
    authRecoveryTimer
    || authRecoveryInFlight
    || authState.pending
    || authState.authenticated
    || !isAuthServiceUnavailable(authState.error)
  ) {
    return;
  }

  authRecoveryTimer = window.setTimeout(async () => {
    authRecoveryTimer = null;
    authRecoveryInFlight = true;

    try {
      if (isFileModeApp()) {
        const redirected = await redirectToLiveHarborCommand();
        if (redirected) {
          return;
        }
      }

      const serviceAvailable = await probeAuthService();
      if (serviceAvailable) {
        await bootstrapAuth();
        return;
      }
    } finally {
      authRecoveryInFlight = false;
    }

    scheduleAuthRecovery();
  }, AUTH_SERVICE_RECOVERY_DELAY_MS);
}

function triggerAuthRecoveryCheck() {
  if (!authState.authenticated && !authState.pending && isAuthServiceUnavailable(authState.error)) {
    void bootstrapAuth();
  }
}

function isSetupMode() {
  return !authState.hasUsers || authState.mode === "setup";
}

function isInviteMode() {
  return Boolean(inviteState.token);
}

function canManageUsers() {
  return USER_ADMIN_ROLES.includes(authState.user?.role || "");
}

function roleHasFullFleetAccess(role) {
  return USER_ADMIN_ROLES.includes(String(role || "").trim());
}

function isOwnerReadOnly() {
  return String(authState.user?.role || "").trim() === "Owner";
}

function getManageableVesselsForAccessModal() {
  if (Array.isArray(managedUsersState.availableVessels) && managedUsersState.availableVessels.length) {
    return managedUsersState.availableVessels;
  }

  return state.vessels.map((vessel) => ({
    id: String(vessel.id),
    name: vessel.name,
    location: vessel.location || vessel.berth || "",
    berth: vessel.berth || "",
    status: vessel.status || "",
  }));
}

function readCheckedVesselIds(container) {
  if (!container) {
    return [];
  }

  return Array.from(container.querySelectorAll('input[type="checkbox"][data-vessel-access-checkbox]:checked'))
    .map((input) => String(input.value || "").trim())
    .filter(Boolean);
}

function renderVesselAccessCheckboxes(vessels, selectedIds, options = {}) {
  const allowEmpty = Boolean(options.allowEmpty);
  const disabled = Boolean(options.disabled);
  const inputName = options.inputName || "vessel-access";
  const emptyMessage = options.emptyMessage || "Add a vessel first, then assign access here.";
  const selectionSet = new Set((Array.isArray(selectedIds) ? selectedIds : []).map((value) => String(value)));

  if (!vessels.length) {
    return `<div class="empty-state compact-empty-state">${escapeHtml(emptyMessage)}</div>`;
  }

  if (!selectionSet.size && !allowEmpty) {
    vessels.forEach((vessel) => selectionSet.add(String(vessel.id)));
  }

  return `<div class="access-vessel-grid">${vessels.map((vessel) => `
    <label class="access-vessel-chip${selectionSet.has(String(vessel.id)) ? " checked" : ""}${disabled ? " disabled" : ""}">
      <input
        type="checkbox"
        name="${escapeHtml(inputName)}"
        value="${escapeHtml(String(vessel.id))}"
        data-vessel-access-checkbox
        ${selectionSet.has(String(vessel.id)) ? "checked" : ""}
        ${disabled ? "disabled" : ""}
      />
      <span class="access-vessel-copy">
        <strong>${escapeHtml(vessel.name)}</strong>
        <small>${escapeHtml(vessel.location || vessel.berth || "No location set")}</small>
      </span>
    </label>
  `).join("")}</div>`;
}

function describeVesselAccess(vesselIds, vessels) {
  const selectedIds = new Set((Array.isArray(vesselIds) ? vesselIds : []).map((value) => String(value)));
  const matches = vessels.filter((vessel) => selectedIds.has(String(vessel.id)));

  if (!matches.length) {
    return "No vessel access assigned.";
  }

  return matches.map((vessel) => vessel.name).join(", ");
}

function ensureAccessibleView() {
  if (!VIEW_OPTIONS.includes(state.activeView)) {
    state.activeView = "overview";
  }

  if (!CHARTERS_TAB_ENABLED && state.activeView === "charters") {
    state.activeView = "overview";
  }

  if (!MAINTENANCE_WORKSPACE_VIEWS.includes(state.activeMaintenanceWorkspace)) {
    state.activeMaintenanceWorkspace = "overview";
  }
}

function getInviteTokenFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return params.get("invite") || "";
}

function clearInviteTokenFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete("invite");
  window.history.replaceState({}, "", url);
}

function buildInviteLink(token) {
  const url = new URL(window.location.href);
  url.search = "";
  url.hash = "";
  url.searchParams.set("invite", token);
  return url.toString();
}

function buildInviteMailto(email, link, role) {
  const subject = encodeURIComponent("Harbor Command invite");
  const body = encodeURIComponent(
    `You have been invited to Harbor Command as ${role}.\n\nOpen this link to create your password and activate access:\n${link}`
  );
  return `mailto:${encodeURIComponent(email)}?subject=${subject}&body=${body}`;
}

function normalizeInviteDelivery(delivery) {
  return {
    ready: Boolean(delivery?.ready),
    provider: delivery?.provider || "Resend",
    fromEmail: delivery?.fromEmail || "",
    publicAppUrl: delivery?.publicAppUrl || "",
    message: delivery?.message || "",
  };
}

function renderAuthShell() {
  const setupMode = isSetupMode();
  const inviteMode = isInviteMode();
  const isLocked = !authState.authenticated;
  const authErrorMessage = inviteMode ? inviteState.error : authState.error;
  const showServiceHelp = !inviteMode && isAuthServiceUnavailable(authErrorMessage) && !authState.pending;
  const showBootSplash = authState.pending && !inviteMode;

  document.body.classList.toggle("auth-locked", isLocked);
  setBootSplashVisible(
    showBootSplash,
    authState.authenticated ? "Refreshing your vessel dashboard." : "Connecting to your vessel workspace."
  );
  elements.authShell.hidden = !isLocked || showBootSplash;
  elements.logoutButton.hidden = !authState.authenticated;
  elements.openAccessModal.hidden = !authState.authenticated || !canManageUsers();

  if (!isLocked) {
    clearAuthRecoveryTimer();
    setBootSplashVisible(false);
    return;
  }

  if (showBootSplash) {
    clearAuthRecoveryTimer();
    return;
  }

  elements.authNameRow.hidden = !(setupMode || inviteMode);
  elements.authRoleRow.hidden = !(setupMode || inviteMode);
  elements.authRoleSelect.disabled = inviteMode;
  elements.authEmailInput.readOnly = inviteMode;
  elements.authPasswordInput.autocomplete = inviteMode ? "new-password" : "current-password";
  elements.authPasswordLabel.textContent = inviteMode ? "Create password" : "Password";
  elements.authSubmit.disabled = authState.pending || inviteState.pending;

  if (authErrorMessage) {
    elements.authError.hidden = false;
    elements.authError.textContent = authErrorMessage;
  } else {
    elements.authError.hidden = true;
    elements.authError.textContent = "";
  }

  if (elements.authServiceHelp) {
    elements.authServiceHelp.hidden = !showServiceHelp;
  }

  if (elements.authOpenLocal) {
    elements.authOpenLocal.href = buildFreshAppUrl("manual");
  }

  if (showServiceHelp) {
    elements.authCopy.textContent = isFileModeApp()
      ? "Use the live Harbor Command app window for sign-in. This file view will reconnect automatically when the local service comes back."
      : "The local Harbor Command service looks offline. This screen will keep retrying automatically and reconnect when the service is available again.";
    scheduleAuthRecovery();
  } else {
    clearAuthRecoveryTimer();
  }

  if (inviteMode) {
    const invite = inviteState.invite;

    elements.authEyebrow.textContent = "Invitation";
    elements.authTitle.textContent = "Accept your Harbor Command invite";
    elements.authCopy.textContent = inviteState.loading
      ? "Checking your invite link."
      : invite
        ? `Create your password to join Harbor Command as ${invite.role}.`
        : "This invite needs attention before you can continue.";
    elements.authSubmit.textContent = inviteState.pending ? "Creating account..." : "Create account";
    elements.authToggle.hidden = false;
    elements.authToggle.textContent = "Back to sign in";
    elements.authEmailInput.value = invite?.email || "";
    elements.authRoleSelect.value = invite?.role || "Crew";
    if (invite && !elements.authNameInput.value) {
      elements.authNameInput.value = invite.name || "";
    }
    elements.authForm.hidden = inviteState.loading || !invite;
    return;
  }

  elements.authForm.hidden = false;

  if (authState.pending) {
    elements.authEyebrow.textContent = "Secure Access";
    elements.authTitle.textContent = "Loading Harbor Command";
    elements.authCopy.textContent = "Checking the vessel access session.";
    elements.authForm.hidden = true;
    elements.authToggle.hidden = true;
    return;
  }

  if (setupMode) {
    elements.authEyebrow.textContent = "First-Time Setup";
    elements.authTitle.textContent = "Create the first vessel account";
    elements.authCopy.textContent = "Set up the captain or management login before the vessel dashboard opens.";
    elements.authSubmit.textContent = "Create account";
    elements.authToggle.hidden = !authState.hasUsers;
    elements.authToggle.textContent = "Already have an account? Log in";
    return;
  }

  elements.authEyebrow.textContent = "Secure Access";
  elements.authTitle.textContent = "Sign in to Harbor Command";
  elements.authCopy.textContent = "Use your vessel email and password to open the dashboard.";
  elements.authSubmit.textContent = "Log in";
  elements.authToggle.hidden = true;
  elements.authToggle.textContent = "";
}

function handleUnauthorizedResponse(message = "Your session expired. Please sign in again.") {
  if (apiSyncTimer) {
    clearTimeout(apiSyncTimer);
    apiSyncTimer = null;
  }

  apiPersistQueued = false;
  apiPersistInFlight = false;
  pendingStateSnapshot = null;
  setAuthState({
    authenticated: false,
    pending: false,
    user: null,
    mode: authState.hasUsers ? "login" : "setup",
    error: message,
  });
  clearPendingUiScrollState();
  clearCachedState();
  clearManagedUsersState();
  clearManagedInvitesState();
  closeAccessModal();
  renderAuthShell();
}

async function apiFetch(path, options = {}) {
  const response = await fetchFromHarborApi(path, options);

  if (response.status === 401) {
    handleUnauthorizedResponse();
    throw new Error("Authentication required.");
  }

  return response;
}

async function loadManagedUsers() {
  if (!canManageUsers()) {
    clearManagedUsersState();
    renderAccessModal();
    return;
  }

  setManagedUsersState({
    loading: true,
    error: "",
  });
  renderAccessModal();

  try {
    const response = await apiFetch("/users");
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload?.error || "Unable to load users.");
    }

    setManagedUsersState({
      items: Array.isArray(payload?.users) ? payload.users : [],
      availableVessels: Array.isArray(payload?.vessels) ? payload.vessels : [],
      loading: false,
      error: "",
    });
  } catch (error) {
    setManagedUsersState({
      loading: false,
      error: error instanceof Error ? error.message : "Unable to load users.",
    });
  }

  renderAccessModal();
}

async function loadManagedInvites() {
  if (!canManageUsers()) {
    clearManagedInvitesState();
    renderAccessModal();
    return;
  }

  setManagedInvitesState({
    loading: true,
    error: "",
  });
  renderAccessModal();

  try {
    const response = await apiFetch("/invites");
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload?.error || "Unable to load invites.");
    }

    setManagedInvitesState({
      items: Array.isArray(payload?.invites) ? payload.invites : [],
      loading: false,
      error: "",
      delivery: normalizeInviteDelivery(payload?.delivery),
    });
  } catch (error) {
    setManagedInvitesState({
      loading: false,
      error: error instanceof Error ? error.message : "Unable to load invites.",
    });
  }

  renderAccessModal();
}

function closeAccessModal() {
  if (elements.accessModal) {
    elements.accessModal.hidden = true;
  }
}

async function openAccessModal() {
  if (!canManageUsers()) {
    return;
  }

  elements.accessModal.hidden = false;
  renderAccessModal();
  await Promise.all([loadManagedUsers(), loadManagedInvites()]);
}

async function loadInviteFromToken(token) {
  clearAuthForm();
  clearManagedUsersState();
  clearManagedInvitesState();
  setInviteState({
    token,
    loading: true,
    pending: false,
    invite: null,
    error: "",
  });
  setAuthState({
    authenticated: false,
    pending: false,
    user: null,
    mode: "invite",
    error: "",
  });
  renderAuthShell();

  try {
    const response = await fetchFromHarborApi(`/invite/${encodeURIComponent(token)}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload?.error || "This invite link is no longer valid.");
    }

    setInviteState({
      loading: false,
      invite: payload?.invite || null,
      error: "",
    });
  } catch (error) {
    setInviteState({
      loading: false,
      invite: null,
      error: error instanceof Error ? error.message : "This invite link is no longer valid.",
    });
  }

  renderAuthShell();
}

async function bootstrapAuth() {
  clearAuthRecoveryTimer();

  if (redirectToCanonicalLocalOrigin()) {
    return;
  }

  if (isFileModeApp()) {
    const redirected = await redirectToLiveHarborCommand();
    if (redirected) {
      return;
    }
  }

  const inviteToken = getInviteTokenFromUrl();
  if (inviteToken) {
    await loadInviteFromToken(inviteToken);
    return;
  }

  clearInviteState();
  setAuthState({
    pending: true,
    error: "",
  });
  renderAuthShell();

  try {
    const response = await fetchFromHarborApi("/auth/status");

    if (!response.ok) {
      throw new Error(`Auth status request failed with status ${response.status}`);
    }

    const payload = await response.json();
    setAuthState({
      hasUsers: Boolean(payload?.hasUsers),
      authenticated: Boolean(payload?.authenticated),
      user: payload?.user || null,
      mode: payload?.authenticated ? "authenticated" : payload?.hasUsers ? "login" : "setup",
      error: "",
    });

    if (authState.authenticated) {
      renderApp();
      await bootstrapApiState();
    }
  } catch (error) {
    setAuthState({
      authenticated: false,
      error: AUTH_SERVICE_UNAVAILABLE_MESSAGE,
    });
  } finally {
    setAuthState({ pending: false });
    renderAuthShell();
    if (authState.authenticated) {
      restorePendingUiScrollState();
    } else {
      clearPendingUiScrollState();
    }
  }
}

async function bootstrapApiState() {
  try {
    const response = await apiFetch("/bootstrap");

    if (!response.ok) {
      throw new Error(`Bootstrap request failed with status ${response.status}`);
    }

    const payload = await response.json();
    setApiAvailability(true);

    if (payload?.catalog) {
      setVesselCatalogState(payload.catalog);
    }

    if (payload?.hasData && payload.state) {
      replaceState(mergeCachedUiPreferences(payload.state, state));
      hydrateState();
      const hydratedSnapshot = snapshotStateForPersistence();
      cacheLocalState(hydratedSnapshot);
      if (authState.authenticated) {
        queueApiPersist(hydratedSnapshot);
        void flushApiPersistQueue();
      }
      renderApp();
      return;
    }

    await saveStateSnapshot(snapshotStateForPersistence());
  } catch (error) {
    setApiAvailability(false, error);
  }
}

function sortWeeklyReportsDescending(reports) {
  return normalizeReportCollection(reports, [])
    .slice()
    .sort((left, right) => right.weekStart.localeCompare(left.weekStart));
}

function syncWeeklyReportsState(reports, preferredReportId = "") {
  state.reports = sortWeeklyReportsDescending(reports);
  const preferred = preferredReportId || state.activeReportId;
  const matchingPreferred = state.reports.find((report) => report.id === preferred);
  const currentWeekReport = state.reports.find((report) => {
    const currentWeek = getActiveWorkWeekRange();
    return report.weekStart === currentWeek.start && report.weekEnd === currentWeek.end;
  });
  state.activeReportId = matchingPreferred?.id || currentWeekReport?.id || state.reports[0]?.id || "";

  if (!state.vesselBundles[state.activeVesselId]) {
    state.vesselBundles[state.activeVesselId] = createFreshVesselBundle();
  }
  state.vesselBundles[state.activeVesselId].reports = cloneItems(state.reports);
}

function upsertWeeklyReportState(report, options = {}) {
  const { preferredReportId = report?.id || "" } = options;
  const normalizedReport = normalizeWeeklyReport(report);
  const existingIndex = state.reports.findIndex((item) => item.id === normalizedReport.id);
  const nextReports = state.reports.slice();

  if (existingIndex >= 0) {
    nextReports[existingIndex] = normalizedReport;
  } else {
    nextReports.unshift(normalizedReport);
  }

  syncWeeklyReportsState(nextReports, preferredReportId);
}

async function createWeeklyReportRequest(weekStart) {
  const response = await apiFetch("/weekly-reports", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      vesselId: Number(state.activeVesselId),
      weekStart,
      status: "draft",
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error || "Unable to create weekly report.");
  }
  return normalizeWeeklyReport(payload.report);
}

async function generateWeeklyReportRequest(weekStart) {
  const response = await apiFetch("/weekly-reports/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      vesselId: Number(state.activeVesselId),
      weekStart,
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error || "Unable to generate weekly report.");
  }
  return normalizeWeeklyReport(payload.report);
}

async function updateWeeklyReportRequest(reportId, nextStatus) {
  const response = await apiFetch(`/weekly-reports/${encodeURIComponent(reportId)}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      status: nextStatus,
    }),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error || "Unable to update weekly report.");
  }
  return normalizeWeeklyReport(payload.report);
}

async function createWeeklyReportEntryRequest(reportId, entryValues) {
  const response = await apiFetch(`/weekly-reports/${encodeURIComponent(reportId)}/entries`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(entryValues),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error || "Unable to save weekly report entry.");
  }
  return normalizeWeeklyReport(payload.report);
}

async function updateWeeklyReportEntryRequest(entryId, entryValues) {
  const response = await apiFetch(`/weekly-report-entries/${encodeURIComponent(entryId)}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(entryValues),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error || "Unable to update weekly report entry.");
  }
  return normalizeWeeklyReport(payload.report);
}

async function deleteWeeklyReportEntryRequest(entryId) {
  const response = await apiFetch(`/weekly-report-entries/${encodeURIComponent(entryId)}`, {
    method: "DELETE",
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload?.error || "Unable to delete weekly report entry.");
  }
  return normalizeWeeklyReport(payload.report);
}

function setApiAvailability(isAvailable, error) {
  apiAvailable = isAvailable;

  if (isAvailable) {
    apiWarningShown = false;
    return;
  }

  if (!apiWarningShown) {
    console.warn("Harbor Command backend unavailable. Using browser storage fallback.", error);
    apiWarningShown = true;
  }
}

function buildPhotoStrippedSnapshot(snapshot) {
  const reducedSnapshot = JSON.parse(JSON.stringify(snapshot));

  if (reducedSnapshot.vessel && typeof reducedSnapshot.vessel === "object") {
    reducedSnapshot.vessel.photoDataUrl = "";
  }

  if (Array.isArray(reducedSnapshot.vessels)) {
    reducedSnapshot.vessels = reducedSnapshot.vessels.map((vessel) => ({
      ...vessel,
      photoDataUrl: "",
    }));
  }

  return reducedSnapshot;
}

function cacheLocalState(snapshotOverride = null) {
  const snapshot = snapshotOverride || snapshotStateForPersistence();
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(snapshot));
  } catch (error) {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(buildPhotoStrippedSnapshot(snapshot)));
      console.warn("Harbor Command local cache exceeded browser storage. Saved a reduced snapshot without vessel photos.", error);
    } catch (reducedError) {
      console.warn("Unable to cache Harbor Command state locally.", reducedError);
    }
  }
}

function snapshotStateForPersistence(options = {}) {
  const { stripPhotos = false } = options;
  syncConnectedAutomation(state);
  syncActiveRuntimeState(state);
  const snapshot = JSON.parse(JSON.stringify(state));

  if (!stripPhotos) {
    return snapshot;
  }

  if (snapshot.vessel && typeof snapshot.vessel === "object") {
    snapshot.vessel.photoDataUrl = "";
  }

  if (Array.isArray(snapshot.vessels)) {
    snapshot.vessels = snapshot.vessels.map((vessel) => ({
      ...vessel,
      photoDataUrl: "",
    }));
  }

  return snapshot;
}

function handlePageExit() {
  storePendingUiScrollState();
  flushPendingPersistOnPageExit();
}

function flushPendingPersistOnPageExit() {
  if (!authState.authenticated || !apiAvailable || (!apiPersistQueued && !pendingStateSnapshot)) {
    return;
  }

  if (apiSyncTimer) {
    clearTimeout(apiSyncTimer);
    apiSyncTimer = null;
  }

  const fullSnapshot = pendingStateSnapshot || snapshotStateForPersistence();
  let requestBody = "";

  try {
    requestBody = JSON.stringify(fullSnapshot);
  } catch (error) {
    return;
  }

  apiPersistQueued = false;
  pendingStateSnapshot = fullSnapshot;

  try {
    fetchFromHarborApi("/bootstrap", {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      keepalive: true,
      body: requestBody,
    });
  } catch (error) {
    // Ignore shutdown-time persistence failures.
  }
}

function queueApiPersist(snapshotOverride = null) {
  pendingStateSnapshot = snapshotOverride || snapshotStateForPersistence();
  apiPersistQueued = true;

  if (apiSyncTimer) {
    clearTimeout(apiSyncTimer);
  }

  apiSyncTimer = window.setTimeout(() => {
    apiSyncTimer = null;
    void flushApiPersistQueue();
  }, API_SYNC_DELAY_MS);
}

async function flushApiPersistQueue() {
  if (!apiPersistQueued || apiPersistInFlight) {
    return;
  }

  apiPersistQueued = false;
  apiPersistInFlight = true;
  const snapshot = pendingStateSnapshot;

  try {
    await saveStateSnapshot(snapshot);
  } catch (error) {
    setApiAvailability(false, error);
  } finally {
    apiPersistInFlight = false;

    if (apiPersistQueued) {
      void flushApiPersistQueue();
    }
  }
}

async function saveStateSnapshot(snapshot) {
  const response = await apiFetch("/bootstrap", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(snapshot),
  });

  if (!response.ok) {
    throw new Error(`Persist request failed with status ${response.status}`);
  }

  setApiAvailability(true);
}

function mergeCachedUiPreferences(serverState, clientState) {
  const mergedState = {
    ...serverState,
  };

  if (VIEW_OPTIONS.includes(clientState?.activeView)) {
    mergedState.activeView = clientState.activeView;
  }

  if (MAINTENANCE_FILTERS.includes(clientState?.activeMaintenanceFilter)) {
    mergedState.activeMaintenanceFilter = clientState.activeMaintenanceFilter;
  }

  if (MAINTENANCE_WORKSPACE_VIEWS.includes(clientState?.activeMaintenanceWorkspace)) {
    mergedState.activeMaintenanceWorkspace = clientState.activeMaintenanceWorkspace;
  }

  if (typeof clientState?.activeMaintenanceCategory === "string") {
    mergedState.activeMaintenanceCategory = clientState.activeMaintenanceCategory;
  }

  if (typeof clientState?.activeMaintenanceQuery === "string") {
    mergedState.activeMaintenanceQuery = clientState.activeMaintenanceQuery;
  }

  if (typeof clientState?.activeReportId === "string") {
    mergedState.activeReportId = clientState.activeReportId;
  }

  if (typeof clientState?.activeWorkWeekStart === "string") {
    mergedState.activeWorkWeekStart = clientState.activeWorkWeekStart;
  }

  if (WORK_ORDER_SORT_OPTIONS.includes(clientState?.activeWorkOrderSort)) {
    mergedState.activeWorkOrderSort = clientState.activeWorkOrderSort;
  }

  if (VENDOR_FILTERS.includes(clientState?.activeVendorFilter)) {
    mergedState.activeVendorFilter = clientState.activeVendorFilter;
  }

  if (MAINTENANCE_SORT_OPTIONS.includes(clientState?.activeMaintenanceSort)) {
    mergedState.activeMaintenanceSort = clientState.activeMaintenanceSort;
  }

  if (VENDOR_SORT_OPTIONS.includes(clientState?.activeVendorSort)) {
    mergedState.activeVendorSort = clientState.activeVendorSort;
  }

  if (INVENTORY_SORT_OPTIONS.includes(clientState?.activeInventorySort)) {
    mergedState.activeInventorySort = clientState.activeInventorySort;
  }

  if (EXPENSE_SORT_OPTIONS.includes(clientState?.activeExpenseSort)) {
    mergedState.activeExpenseSort = clientState.activeExpenseSort;
  }

  if (CHARTER_SORT_OPTIONS.includes(clientState?.activeCharterSort)) {
    mergedState.activeCharterSort = clientState.activeCharterSort;
  }

  if (VOYAGE_SORT_OPTIONS.includes(clientState?.activeVoyageSort)) {
    mergedState.activeVoyageSort = clientState.activeVoyageSort;
  }

  if (
    clientState?.activeVesselId
    && Array.isArray(mergedState.vessels)
    && mergedState.vessels.some((vessel) => String(vessel.id) === String(clientState.activeVesselId))
  ) {
    mergedState.activeVesselId = String(clientState.activeVesselId);
  }

  return mergedState;
}

function getAllReportEntries(reports = state.reports) {
  return (Array.isArray(reports) ? reports : [])
    .flatMap((report) => (
      Array.isArray(report?.entries)
        ? report.entries.map((entry) => ({
            ...entry,
            reportId: report.id,
          }))
        : []
    ))
    .map((entry) => ({
      ...entry,
      reportId: entry.reportId,
    }));
}

function normalizeActiveWorkWeekStart(preferredStart = "") {
  const calendarWeek = getWorkWeekRange(new Date());
  const normalizedPreferred = typeof preferredStart === "string" && preferredStart.trim()
    ? getWorkWeekRange(preferredStart).start
    : "";

  if (!normalizedPreferred) {
    return calendarWeek.start;
  }

  if (compareDateStrings(normalizedPreferred, calendarWeek.start) < 0) {
    return calendarWeek.start;
  }

  return normalizedPreferred;
}

function getCurrentWeekRange(referenceDate = null, preferredStart = "") {
  if (referenceDate !== null && typeof referenceDate !== "undefined") {
    return getWorkWeekRange(referenceDate);
  }

  return getWorkWeekRange(normalizeActiveWorkWeekStart(preferredStart));
}

function getActiveWorkWeekRange() {
  return getCurrentWeekRange(null, state.activeWorkWeekStart);
}

function getCurrentWeekReport() {
  const currentWeek = getActiveWorkWeekRange();
  return state.reports.find((report) => report.weekStart === currentWeek.start && report.weekEnd === currentWeek.end) || null;
}

function getSelectedWeeklyReport() {
  const selected = state.reports.find((report) => report.id === state.activeReportId);
  if (selected) {
    return selected;
  }

  const currentWeekReport = getCurrentWeekReport();
  if (currentWeekReport) {
    return currentWeekReport;
  }

  return state.reports[0] || null;
}

function ensureActiveReportSelection() {
  const selectedReport = getSelectedWeeklyReport();
  state.activeReportId = selectedReport?.id || "";
}

function getSelectedReportEntries() {
  return getSelectedWeeklyReport()?.entries || [];
}

function readPendingUiScrollState() {
  try {
    const rawState = sessionStorage.getItem(UI_SCROLL_STORAGE_KEY);
    if (!rawState) {
      return null;
    }

    const parsed = JSON.parse(rawState);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (error) {
    return null;
  }
}

function storePendingUiScrollState() {
  if (!authState.authenticated || document.body.classList.contains("auth-locked")) {
    return;
  }

  try {
    sessionStorage.setItem(
      UI_SCROLL_STORAGE_KEY,
      JSON.stringify({
        activeView: state.activeView,
        scrollY: window.scrollY,
      })
    );
  } catch (error) {
    // Ignore browsers that block sessionStorage during shutdown.
  }
}

function clearPendingUiScrollState() {
  pendingUiScrollState = null;
  try {
    sessionStorage.removeItem(UI_SCROLL_STORAGE_KEY);
  } catch (error) {
    // Ignore storage cleanup failures.
  }
}

function restorePendingUiScrollState() {
  if (!pendingUiScrollState) {
    return;
  }

  const savedScrollState = pendingUiScrollState;
  clearPendingUiScrollState();

  if (savedScrollState.activeView !== state.activeView) {
    return;
  }

  const scrollY = Number(savedScrollState.scrollY);
  if (!Number.isFinite(scrollY) || scrollY <= 0) {
    return;
  }

  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(() => {
      window.scrollTo({
        top: scrollY,
        behavior: "auto",
      });
    });
  });
}

function normalizeLoadedState(parsed, baseState) {
  const normalized = Array.isArray(parsed.yachts) ? migrateFleetState(parsed, baseState) : parsed;
  const maintenanceAssets = normalizeMaintenanceAssetCollection(normalized.maintenanceAssets, baseState.maintenanceAssets);
  const maintenanceItems = extractRecurringMaintenance(normalized.maintenance, baseState.maintenance);
  const maintenanceHistory = normalizeMaintenanceHistoryCollection(normalized.maintenanceHistory, baseState.maintenanceHistory);
  const workOrderItems = extractLegacyWorkOrders(normalized.workOrders, normalized.maintenance, baseState.workOrders);
  const hydratedState = normalizeRuntimeFleetState({
    ...baseState,
    ...normalized,
    vessel: normalizeVesselState(normalized.vessel, baseState.vessel),
    maintenanceAssets,
    maintenance: normalizeMaintenanceCollection(maintenanceItems, baseState.maintenance),
    maintenanceHistory,
    workOrders: normalizeWorkOrderCollection(workOrderItems, baseState.workOrders),
    inventory: normalizeCollection(normalized.inventory, baseState.inventory),
    expenses: normalizeCollection(normalized.expenses, baseState.expenses),
    charters: normalizeCollection(normalized.charters, baseState.charters),
    crew: normalizeCollection(normalized.crew, baseState.crew),
    reports: normalizeReportCollection(normalized.reports, baseState.reports),
    vendors: normalizeCollection(normalized.vendors, baseState.vendors),
    voyages: normalizeCollection(normalized.voyages, baseState.voyages),
    activeView: VIEW_OPTIONS.includes(normalized.activeView) ? normalized.activeView : "overview",
    activeMaintenanceWorkspace: MAINTENANCE_WORKSPACE_VIEWS.includes(normalized.activeMaintenanceWorkspace)
      ? normalized.activeMaintenanceWorkspace
      : "overview",
    activeMaintenanceFilter: MAINTENANCE_FILTERS.includes(normalized.activeMaintenanceFilter)
      ? normalized.activeMaintenanceFilter
      : "all",
    activeMaintenanceCategory: typeof normalized.activeMaintenanceCategory === "string"
      ? normalized.activeMaintenanceCategory
      : getDefaultMaintenanceCategory(),
    activeMaintenanceQuery: typeof normalized.activeMaintenanceQuery === "string"
      ? normalized.activeMaintenanceQuery
      : "",
    activeReportId: typeof normalized.activeReportId === "string" ? normalized.activeReportId : "",
    activeWorkWeekStart: normalizeActiveWorkWeekStart(normalized.activeWorkWeekStart),
    activeWorkOrderSort: WORK_ORDER_SORT_OPTIONS.includes(normalized.activeWorkOrderSort)
      ? normalized.activeWorkOrderSort
      : "date-asc",
    activeMaintenanceSort: MAINTENANCE_SORT_OPTIONS.includes(normalized.activeMaintenanceSort)
      ? normalized.activeMaintenanceSort
      : "category",
    activeVendorFilter: VENDOR_FILTERS.includes(normalized.activeVendorFilter)
      ? normalized.activeVendorFilter
      : "all",
    activeVendorSort: VENDOR_SORT_OPTIONS.includes(normalized.activeVendorSort)
      ? normalized.activeVendorSort
      : "name-asc",
    activeInventorySort: INVENTORY_SORT_OPTIONS.includes(normalized.activeInventorySort)
      ? normalized.activeInventorySort
      : "name-asc",
    activeExpenseSort: EXPENSE_SORT_OPTIONS.includes(normalized.activeExpenseSort)
      ? normalized.activeExpenseSort
      : "date-desc",
    activeCharterSort: CHARTER_SORT_OPTIONS.includes(normalized.activeCharterSort)
      ? normalized.activeCharterSort
      : "start-asc",
    activeVoyageSort: VOYAGE_SORT_OPTIONS.includes(normalized.activeVoyageSort)
      ? normalized.activeVoyageSort
      : "departure-asc",
  }, baseState);

  hydratedState.activeView = VIEW_OPTIONS.includes(normalized.activeView) ? normalized.activeView : "overview";
  hydratedState.activeMaintenanceWorkspace = MAINTENANCE_WORKSPACE_VIEWS.includes(normalized.activeMaintenanceWorkspace)
    ? normalized.activeMaintenanceWorkspace
    : "overview";
  hydratedState.activeMaintenanceFilter = MAINTENANCE_FILTERS.includes(normalized.activeMaintenanceFilter)
    ? normalized.activeMaintenanceFilter
    : "all";
  hydratedState.activeMaintenanceCategory = typeof normalized.activeMaintenanceCategory === "string"
    ? normalized.activeMaintenanceCategory
    : getDefaultMaintenanceCategory();
  hydratedState.activeMaintenanceQuery = typeof normalized.activeMaintenanceQuery === "string"
    ? normalized.activeMaintenanceQuery
    : "";
  hydratedState.activeReportId = typeof normalized.activeReportId === "string" ? normalized.activeReportId : "";
  hydratedState.activeWorkWeekStart = normalizeActiveWorkWeekStart(normalized.activeWorkWeekStart);
  hydratedState.activeWorkOrderSort = WORK_ORDER_SORT_OPTIONS.includes(normalized.activeWorkOrderSort)
    ? normalized.activeWorkOrderSort
    : "date-asc";
  hydratedState.activeMaintenanceSort = MAINTENANCE_SORT_OPTIONS.includes(normalized.activeMaintenanceSort)
    ? normalized.activeMaintenanceSort
    : "category";
  hydratedState.activeVendorFilter = VENDOR_FILTERS.includes(normalized.activeVendorFilter)
    ? normalized.activeVendorFilter
    : "all";
  hydratedState.activeVendorSort = VENDOR_SORT_OPTIONS.includes(normalized.activeVendorSort)
    ? normalized.activeVendorSort
    : "name-asc";
  hydratedState.activeInventorySort = INVENTORY_SORT_OPTIONS.includes(normalized.activeInventorySort)
    ? normalized.activeInventorySort
    : "name-asc";
  hydratedState.activeExpenseSort = EXPENSE_SORT_OPTIONS.includes(normalized.activeExpenseSort)
    ? normalized.activeExpenseSort
    : "date-desc";
  hydratedState.activeCharterSort = CHARTER_SORT_OPTIONS.includes(normalized.activeCharterSort)
    ? normalized.activeCharterSort
    : "start-asc";
  hydratedState.activeVoyageSort = VOYAGE_SORT_OPTIONS.includes(normalized.activeVoyageSort)
    ? normalized.activeVoyageSort
    : "departure-asc";

  return hydratedState;
}

function migrateFleetState(parsed, baseState) {
  const activeYacht = parsed.yachts.find((yacht) => yacht.id === parsed.selectedYachtId) || parsed.yachts[0];

  return {
    vessel: {
      ...baseState.vessel,
      ...stripYachtReference(activeYacht),
    },
    maintenanceAssets: [],
    maintenance: cloneItems(baseState.maintenance),
    maintenanceHistory: [],
    workOrders: collectItemsForVessel(parsed.maintenance, activeYacht.id, baseState.workOrders),
    charters: collectItemsForVessel(parsed.charters, activeYacht.id, baseState.charters),
    crew: collectItemsForVessel(parsed.crew, activeYacht.id, baseState.crew),
    reports: collectItemsForVessel(parsed.reports, activeYacht.id, baseState.reports),
    voyages: collectItemsForVessel(parsed.voyages, activeYacht.id, baseState.voyages),
    activeView: parsed.activeView || "overview",
  };
}

function collectItemsForVessel(items, yachtId, fallback) {
  const scopedItems = Array.isArray(items) && items.length ? items : fallback;
  return scopedItems
    .filter((item) => !item.yachtId || item.yachtId === yachtId)
    .map((item) => stripYachtReference(item));
}

function stripYachtReference(item) {
  const clone = { ...item };
  delete clone.id;
  delete clone.yachtId;

  return item.id ? { id: item.id, ...clone } : clone;
}

function normalizeCollection(items, fallback) {
  return Array.isArray(items) ? items : cloneItems(fallback);
}

function normalizeMachineryItem(item, fallback, prefix, index = 0) {
  const merged = {
    ...(fallback || {}),
    ...(item || {}),
  };

  return {
    id: String(merged.id || createId(prefix)),
    label: String(merged.label || `${prefix === "generator" ? "Generator" : "Engine"} ${index + 1}`).trim(),
    manufacturer: String(merged.manufacturer || "").trim(),
    model: String(merged.model || "").trim(),
    rating: String(merged.rating || "").trim(),
    hours: normalizePositiveInteger(merged.hours, fallback?.hours ?? 0, 0),
    lastServiceHours: normalizePositiveInteger(merged.lastServiceHours, fallback?.lastServiceHours ?? 0, 0),
    serviceIntervalHours: normalizePositiveInteger(merged.serviceIntervalHours, fallback?.serviceIntervalHours ?? 0, 0),
    lastServiceDate: String(merged.lastServiceDate || ""),
    nextServiceDate: String(merged.nextServiceDate || ""),
    notes: String(merged.notes || "").trim(),
  };
}

function normalizeMachineryCollection(items, fallback, prefix, legacySummary = "") {
  if (Array.isArray(items)) {
    return items.map((item, index) => normalizeMachineryItem(item, fallback?.[index], prefix, index));
  }

  if (Array.isArray(fallback) && fallback.length) {
    return fallback.map((item, index) => normalizeMachineryItem(item, item, prefix, index));
  }

  const summary = String(legacySummary || "").trim();
  if (!summary) {
    return [];
  }

  return [
    normalizeMachineryItem(
      {
        label: prefix === "generator" ? "Primary generator" : "Primary machinery",
        notes: summary,
      },
      {},
      prefix,
      0
    ),
  ];
}

function buildMachinerySummary(items, fallback = "") {
  if (!Array.isArray(items) || !items.length) {
    return String(fallback || "").trim();
  }

  return items
    .map((item) => {
      const parts = [item.label, item.manufacturer, item.model, item.rating].filter(Boolean);
      const hoursCopy = item.hours ? `${formatNumberValue(item.hours)} hrs` : "";
      return [parts.join(" "), hoursCopy].filter(Boolean).join(" | ");
    })
    .filter(Boolean)
    .join(" | ");
}

function normalizeVesselState(vessel, fallback) {
  const merged = {
    ...fallback,
    ...(vessel || {}),
  };

  if (merged.name === LEGACY_VESSEL_NAME) {
    merged.name = DEFAULT_VESSEL_NAME;
  }

  merged.fuel = clampPercent(merged.fuel);
  merged.waterTank = clampPercent(merged.waterTank);
  merged.greyTank = clampPercent(merged.greyTank);
  merged.blackTankLevel = clampPercent(merged.blackTankLevel);
  merged.batteryStatus = clampPercent(merged.batteryStatus);
  merged.utilization = clampPercent(merged.utilization);
  merged.length = normalizePositiveInteger(merged.length, fallback.length, 0);
  merged.guests = normalizePositiveInteger(merged.guests, fallback.guests, 0);
  merged.yearBuilt = normalizePositiveInteger(merged.yearBuilt, fallback.yearBuilt, 0);
  merged.beam = normalizeNumber(merged.beam, fallback.beam);
  merged.draft = normalizeNumber(merged.draft, fallback.draft);
  merged.fuelCapacity = normalizePositiveInteger(merged.fuelCapacity, fallback.fuelCapacity, 0);
  merged.waterCapacity = normalizePositiveInteger(merged.waterCapacity, fallback.waterCapacity, 0);
  merged.greyWaterCapacity = normalizePositiveInteger(merged.greyWaterCapacity, fallback.greyWaterCapacity, 0);
  merged.blackWaterCapacity = normalizePositiveInteger(merged.blackWaterCapacity, fallback.blackWaterCapacity, 0);
  merged.nextService = typeof merged.nextService === "string"
    ? merged.nextService.trim()
    : String(fallback.nextService || "").trim();
  merged.photoDataUrl = typeof merged.photoDataUrl === "string" ? merged.photoDataUrl : "";
  merged.builder = typeof merged.builder === "string"
    ? merged.builder.trim()
    : String(fallback.builder || "").trim();
  merged.vesselType = typeof merged.vesselType === "string"
    ? merged.vesselType.trim()
    : String(fallback.vesselType || "").trim();
  merged.hullMaterial = typeof merged.hullMaterial === "string"
    ? merged.hullMaterial.trim()
    : String(fallback.hullMaterial || "").trim();
  merged.catalogManufacturerId = String(merged.catalogManufacturerId || "").trim();
  merged.catalogModelId = String(merged.catalogModelId || "").trim();
  merged.catalogSpecId = String(merged.catalogSpecId || "").trim();
  merged.isCustom = merged.isCustom === false ? false : !merged.catalogSpecId;
  merged.engines = normalizeMachineryCollection(
    merged.engines,
    fallback.engines,
    "engine",
    typeof merged.engineInfo === "string" ? merged.engineInfo : fallback.engineInfo
  );
  merged.generators = normalizeMachineryCollection(
    merged.generators,
    fallback.generators,
    "generator",
    typeof merged.generatorInfo === "string" ? merged.generatorInfo : fallback.generatorInfo
  );
  merged.engineInfo = buildMachinerySummary(
    merged.engines,
    typeof merged.engineInfo === "string" ? merged.engineInfo : (fallback.engineInfo || "")
  );
  merged.generatorInfo = buildMachinerySummary(
    merged.generators,
    typeof merged.generatorInfo === "string" ? merged.generatorInfo : (fallback.generatorInfo || "")
  );

  return merged;
}

function normalizeVesselCatalogPayload(payload) {
  const source = payload && typeof payload === "object" ? payload : {};
  const normalizeManufacturerOptions = (items) =>
    Array.isArray(items)
      ? items.map((item) => ({
          id: String(item?.id || "").trim(),
          name: String(item?.name || "").trim(),
          slug: String(item?.slug || "").trim(),
        })).filter((item) => item.id && item.name)
      : [];
  const manufacturers = Array.isArray(source.manufacturers)
    ? normalizeManufacturerOptions(source.manufacturers)
    : [];
  const models = Array.isArray(source.models)
    ? source.models.map((item) => ({
        id: String(item?.id || "").trim(),
        manufacturerId: String(item?.manufacturerId || "").trim(),
        name: String(item?.name || "").trim(),
        vesselType: String(item?.vesselType || "").trim(),
      })).filter((item) => item.id && item.manufacturerId && item.name)
    : [];
  const specs = Array.isArray(source.specs)
    ? source.specs.map((item) => ({
        id: String(item?.id || "").trim(),
        modelId: String(item?.modelId || "").trim(),
        manufacturerId: String(item?.manufacturerId || "").trim(),
        manufacturerName: String(item?.manufacturerName || "").trim(),
        modelName: String(item?.modelName || "").trim(),
        year: normalizePositiveInteger(item?.year, 0, 0),
        vesselType: String(item?.vesselType || "").trim(),
        length: normalizeNumber(item?.length, 0),
        beam: normalizeNumber(item?.beam, 0),
        draft: normalizeNumber(item?.draft, 0),
        fuelCapacity: normalizePositiveInteger(item?.fuelCapacity, 0, 0),
        waterCapacity: normalizePositiveInteger(item?.waterCapacity, 0, 0),
        blackWaterCapacity: normalizePositiveInteger(item?.blackWaterCapacity, 0, 0),
        greyWaterCapacity: normalizePositiveInteger(item?.greyWaterCapacity, 0, 0),
        engineInfo: String(item?.engineInfo || "").trim(),
        generatorInfo: String(item?.generatorInfo || "").trim(),
        hullMaterial: String(item?.hullMaterial || "").trim(),
      })).filter((item) => item.id && item.modelId && item.manufacturerId && item.year)
    : [];
  const years = Array.isArray(source.years)
    ? source.years.map((year) => normalizePositiveInteger(year, 0, 0)).filter(Boolean).sort((left, right) => right - left)
    : Array.from(new Set(specs.map((spec) => spec.year))).sort((left, right) => right - left);
  const maintenanceTemplates = Array.isArray(source.maintenanceTemplates)
    ? source.maintenanceTemplates.map((template) => ({
        id: String(template?.id || "").trim(),
        name: String(template?.name || "").trim(),
        assetType: String(template?.assetType || "").trim(),
        description: String(template?.description || "").trim(),
        tasks: Array.isArray(template?.tasks)
          ? template.tasks.map((task, index) => ({
              id: String(task?.id || `task-${index + 1}`).trim(),
              title: String(task?.title || "").trim(),
              category: String(task?.category || "General").trim(),
              description: String(task?.description || "").trim(),
              defaultPriority: String(task?.defaultPriority || "Medium").trim(),
              intervalDays: normalizePositiveInteger(task?.intervalDays, 0, 0),
              intervalHours: normalizePositiveInteger(task?.intervalHours, 0, 0),
              reminderDays: normalizePositiveInteger(task?.reminderDays, 0, 0),
              reminderHours: normalizePositiveInteger(task?.reminderHours, 0, 0),
              recurrenceMode: String(task?.recurrenceMode || "days").trim().toLowerCase(),
              sortOrder: Number.isFinite(Number(task?.sortOrder)) ? Number(task.sortOrder) : index,
            }))
          : [],
      })).filter((template) => template.id && template.name)
    : [];

  return {
    years,
    manufacturers,
    models,
    specs,
    engineManufacturers: normalizeManufacturerOptions(source.engineManufacturers),
    generatorManufacturers: normalizeManufacturerOptions(source.generatorManufacturers),
    maintenanceTemplates,
  };
}

function setVesselCatalogState(payload) {
  Object.assign(vesselCatalogState, normalizeVesselCatalogPayload(payload), {
    loaded: true,
    loading: false,
    error: "",
  });
}

function getAvailableCatalogManufacturers(selectedYear = addVesselState.year) {
  const year = normalizePositiveInteger(selectedYear, 0, 0);
  const manufacturerIds = new Set(
    vesselCatalogState.specs
      .filter((spec) => !year || spec.year === year)
      .map((spec) => spec.manufacturerId)
  );

  return vesselCatalogState.manufacturers.filter((item) => manufacturerIds.has(item.id));
}

function getAvailableCatalogModels(selectedYear = addVesselState.year, selectedManufacturerId = addVesselState.manufacturerId) {
  const year = normalizePositiveInteger(selectedYear, 0, 0);
  const manufacturerId = String(selectedManufacturerId || "").trim();
  return vesselCatalogState.specs
    .filter((spec) => (!year || spec.year === year) && (!manufacturerId || spec.manufacturerId === manufacturerId))
    .map((spec) => ({
      id: spec.modelId,
      name: spec.modelName,
      manufacturerId: spec.manufacturerId,
      vesselType: spec.vesselType,
    }))
    .filter((item, index, source) => source.findIndex((candidate) => candidate.id === item.id) === index)
    .sort((left, right) => left.name.localeCompare(right.name));
}

function getSelectedCatalogSpec() {
  const year = normalizePositiveInteger(addVesselState.year, 0, 0);
  const manufacturerId = String(addVesselState.manufacturerId || "").trim();
  const modelId = String(addVesselState.modelId || "").trim();

  if (!year || !manufacturerId || !modelId) {
    return null;
  }

  const exactSpec = vesselCatalogState.specs.find((spec) =>
    spec.year === year
    && spec.manufacturerId === manufacturerId
    && spec.modelId === modelId
    && (!addVesselState.specId || spec.id === addVesselState.specId)
  );

  return exactSpec || null;
}

function normalizeMaintenanceAssetItem(item, index = 0) {
  const source = item && typeof item === "object" ? { ...item } : {};
  return {
    ...source,
    id: String(source.id || createId("maintenance-asset")),
    templateId: String(source.templateId || "").trim(),
    name: String(source.name || `Installed asset ${index + 1}`).trim(),
    assetType: String(source.assetType || "").trim(),
    manufacturer: String(source.manufacturer || "").trim(),
    model: String(source.model || "").trim(),
    serialNumber: String(source.serialNumber || source.serial_number || "").trim(),
    location: String(source.location || "").trim(),
    meterSourceType: String(source.meterSourceType || source.meter_source_type || "none").trim() || "none",
    meterSourceId: String(source.meterSourceId || source.meter_source_id || "").trim(),
    currentHours: normalizePositiveInteger(source.currentHours ?? source.current_hours, 0, 0),
    notes: String(source.notes || "").trim(),
    createdAt: source.createdAt || source.created_at || currentIsoStamp(),
    updatedAt: source.updatedAt || source.updated_at || currentIsoStamp(),
    sortOrder: Number.isFinite(Number(source.sortOrder ?? source.sort_order)) ? Number(source.sortOrder ?? source.sort_order) : index,
  };
}

function normalizeMaintenanceAssetCollection(items, fallback) {
  const source = Array.isArray(items) ? items : cloneItems(fallback);
  return source.map((item, index) => normalizeMaintenanceAssetItem(item, index));
}

function normalizeMaintenanceRecurrenceMode(value, intervalDays = 0, intervalHours = 0) {
  const recurrenceMode = String(value || "").trim().toLowerCase();
  if (["days", "hours", "days-or-hours"].includes(recurrenceMode)) {
    return recurrenceMode;
  }

  if (intervalDays > 0 && intervalHours > 0) {
    return "days-or-hours";
  }

  if (intervalHours > 0) {
    return "hours";
  }

  return "days";
}

function normalizeMaintenanceItem(item, index = 0) {
  const source = item && typeof item === "object" ? { ...item } : {};
  if (!("notes" in source) && "owner" in source) {
    source.notes = source.owner;
  }
  delete source.owner;

  const intervalDays = normalizePositiveInteger(
    source.intervalDays ?? source.interval_days,
    inferMaintenanceIntervalDays(source),
    0
  );
  const intervalHours = normalizePositiveInteger(
    source.intervalHours ?? source.interval_hours,
    inferMaintenanceIntervalHours(source),
    0
  );
  const reminderDays = normalizePositiveInteger(
    source.reminderDays ?? source.reminder_days,
    inferMaintenanceReminderDays({ ...source, intervalDays }),
    0
  );
  const reminderHours = normalizePositiveInteger(
    source.reminderHours ?? source.reminder_hours,
    inferMaintenanceReminderHours({ ...source, intervalHours }),
    0
  );
  const recurrenceMode = normalizeMaintenanceRecurrenceMode(
    source.recurrenceMode ?? source.recurrence_mode,
    intervalDays,
    intervalHours
  );

  let status = String(source.status || "Not Started").trim() || "Not Started";
  if (status === "Scheduled") {
    status = "Not Started";
  }
  if (status === "Complete") {
    status = "Completed";
  }

  return {
    ...source,
    id: String(source.id || createId("maintenance-task")),
    title: String(source.title || `Maintenance task ${index + 1}`).trim() || `Maintenance task ${index + 1}`,
    category: String(source.category || "General").trim() || "General",
    status,
    priority: String(source.priority || "Medium").trim() || "Medium",
    assetId: String(source.assetId || source.asset_id || "").trim(),
    templateId: String(source.templateId || source.template_id || "").trim(),
    templateTaskId: String(source.templateTaskId || source.template_task_id || "").trim(),
    dueDate: formatInputDate(source.dueDate || source.due_date || ""),
    dueHours: normalizePositiveInteger(source.dueHours ?? source.due_hours, 0, 0),
    lastCompleted: formatInputDate(source.lastCompleted || source.last_completed || ""),
    lastCompletedHours: normalizePositiveInteger(source.lastCompletedHours ?? source.last_completed_hours, 0, 0),
    intervalDays,
    intervalHours,
    reminderDays,
    reminderHours,
    recurrenceMode,
    meterSourceType: String(source.meterSourceType || source.meter_source_type || "none").trim().toLowerCase() || "none",
    meterSourceId: String(source.meterSourceId || source.meter_source_id || "").trim(),
    isCustom: source.isCustom === false ? false : normalizePositiveInteger(source.isCustom ?? source.is_custom, 1, 0) !== 0,
    notes: String(source.notes || "").trim(),
    createdAt: source.createdAt || source.created_at || currentIsoStamp(),
    updatedAt: source.updatedAt || source.updated_at || currentIsoStamp(),
    sortOrder: Number.isFinite(Number(source.sortOrder ?? source.sort_order)) ? Number(source.sortOrder ?? source.sort_order) : index,
  };
}

function normalizeMaintenanceCollection(items, fallback) {
  const source = Array.isArray(items) ? items : cloneItems(fallback);
  return source.map((item, index) => normalizeMaintenanceItem(item, index));
}

function extractRecurringMaintenance(items, fallback) {
  return Array.isArray(items) ? items : cloneItems(fallback);
}

function normalizeMaintenanceHistoryEntry(item, index = 0) {
  const source = item && typeof item === "object" ? { ...item } : {};
  return {
    ...source,
    id: String(source.id || createId("maintenance-history")),
    vesselId: String(source.vesselId || source.vessel_id || "").trim(),
    maintenanceLogId: String(source.maintenanceLogId || source.maintenance_log_id || "").trim(),
    assetId: String(source.assetId || source.asset_id || "").trim(),
    templateTaskId: String(source.templateTaskId || source.template_task_id || "").trim(),
    workOrderId: String(source.workOrderId || source.work_order_id || "").trim(),
    source: String(source.source || "manual").trim() || "manual",
    completedAt: String(source.completedAt || source.completed_at || currentIsoStamp()).trim(),
    completionDate: formatInputDate(source.completionDate || source.completion_date || source.completedAt || source.completed_at || todayStamp()),
    completedHours: normalizePositiveInteger(source.completedHours ?? source.completed_hours, 0, 0),
    workDone: String(source.workDone || source.work_done || "").trim(),
    systemsChecked: String(source.systemsChecked || source.systems_checked || "").trim(),
    issues: String(source.issues || "").trim(),
    notes: String(source.notes || "").trim(),
    createdAt: source.createdAt || source.created_at || currentIsoStamp(),
    updatedAt: source.updatedAt || source.updated_at || currentIsoStamp(),
    sortOrder: Number.isFinite(Number(source.sortOrder ?? source.sort_order)) ? Number(source.sortOrder ?? source.sort_order) : index,
  };
}

function normalizeMaintenanceHistoryCollection(items, fallback) {
  const source = Array.isArray(items) ? items : cloneItems(fallback);
  return source
    .map((item, index) => normalizeMaintenanceHistoryEntry(item, index))
    .sort((left, right) => parseDateValue(right.completionDate) - parseDateValue(left.completionDate));
}

function normalizeWorkOrderItem(item, index = 0) {
  const source = item && typeof item === "object" ? { ...item } : {};
  const entryTitle = String(source.item || source.title || "General update").trim() || "General update";
  const notes = String(source.notes || source.owner || "").trim();
  const reportDate = formatInputDate(source.reportDate || source.entryDate || source.dueDate || todayStamp());
  const weekRange = getWorkWeekRange(source.weekStart || source.weekEnd || reportDate);

  return {
    ...source,
    id: String(source.id || createId("work-order")),
    item: entryTitle,
    title: entryTitle,
    reportDate,
    dueDate: reportDate,
    workDone: String(source.workDone || "").trim(),
    systemsChecked: String(source.systemsChecked || "").trim(),
    issues: String(source.issues || "").trim(),
    notes,
    weekStart: formatInputDate(source.weekStart || weekRange.start),
    weekEnd: formatInputDate(source.weekEnd || weekRange.end),
    status: String(source.status || "Open"),
    priority: String(source.priority || ""),
    maintenanceLogId: String(source.maintenanceLogId || source.maintenance_log_id || "").trim(),
    originType: normalizeWorkOrderOriginType(source.originType || source.origin_type),
    completedAt: String(source.completedAt || source.completed_at || "").trim(),
    createdAt: source.createdAt || currentIsoStamp(),
    updatedAt: source.updatedAt || currentIsoStamp(),
    sortOrder: Number.isFinite(Number(source.sortOrder)) ? Number(source.sortOrder) : index,
  };
}

function normalizeWorkOrderOriginType(value) {
  return String(value || "").trim().toLowerCase() === "maintenance-suggestion"
    ? "maintenance-suggestion"
    : "manual";
}

function normalizeWorkOrderCollection(items, fallback) {
  const source = Array.isArray(items) ? items : cloneItems(fallback);
  return source.map((item, index) => normalizeWorkOrderItem(item, index));
}

function getMaintenanceTemplateById(templateId) {
  return vesselCatalogState.maintenanceTemplates.find((template) => template.id === String(templateId || "").trim()) || null;
}

function getMaintenanceAssetById(assetId, targetState = state) {
  return (targetState.maintenanceAssets || []).find((asset) => String(asset.id) === String(assetId || "")) || null;
}

function getMaintenanceTaskMeterConfig(task, targetState = state) {
  const linkedAsset = getMaintenanceAssetById(task?.assetId, targetState);
  return {
    meterSourceType: String(task?.meterSourceType || linkedAsset?.meterSourceType || "none").trim() || "none",
    meterSourceId: String(task?.meterSourceId || linkedAsset?.meterSourceId || "").trim(),
    currentHours: linkedAsset ? normalizePositiveInteger(linkedAsset.currentHours, 0, 0) : 0,
  };
}

function getLinkedEngineHours(meterSourceId, targetState = state) {
  return normalizePositiveInteger(
    (targetState.vessel?.engines || []).find((item) => String(item.id) === String(meterSourceId || ""))?.hours,
    0,
    0
  );
}

function getLinkedGeneratorHours(meterSourceId, targetState = state) {
  return normalizePositiveInteger(
    (targetState.vessel?.generators || []).find((item) => String(item.id) === String(meterSourceId || ""))?.hours,
    0,
    0
  );
}

function getMaintenanceCurrentHours(task, targetState = state) {
  const meter = getMaintenanceTaskMeterConfig(task, targetState);
  if (meter.meterSourceType === "engine") {
    return getLinkedEngineHours(meter.meterSourceId, targetState);
  }
  if (meter.meterSourceType === "generator") {
    return getLinkedGeneratorHours(meter.meterSourceId, targetState);
  }
  return normalizePositiveInteger(meter.currentHours, 0, 0);
}

function getMaintenanceIntervalHours(task) {
  return normalizePositiveInteger(task?.intervalHours, inferMaintenanceIntervalHours(task), 0);
}

function getMaintenanceReminderHours(task) {
  return normalizePositiveInteger(task?.reminderHours, inferMaintenanceReminderHours(task), 0);
}

function getMaintenanceDueHours(task) {
  return normalizePositiveInteger(task?.dueHours, 0, 0);
}

function getMaintenanceHoursRemaining(task, targetState = state) {
  const dueHours = getMaintenanceDueHours(task);
  if (!usesHourRecurrence(task) || dueHours <= 0) {
    return null;
  }

  return dueHours - getMaintenanceCurrentHours(task, targetState);
}

function buildMaintenanceTemplateOptions(selectedId = "") {
  const selectedValue = String(selectedId || "").trim();
  const templates = vesselCatalogState.maintenanceTemplates
    .slice()
    .sort((left, right) => String(left.name || "").localeCompare(String(right.name || "")));

  return [
    `<option value="">Custom / no template</option>`,
    ...templates.map((template) => `
      <option value="${escapeHtml(template.id)}" ${selectedValue === template.id ? "selected" : ""}>
        ${escapeHtml(template.name)}
      </option>
    `),
  ].join("");
}

function buildMaintenanceAssetOptions(selectedId = "") {
  const selectedValue = String(selectedId || "").trim();
  return [
    `<option value="">Standalone vessel task</option>`,
    ...state.maintenanceAssets
      .slice()
      .sort((left, right) => compareTextValues(left.name, right.name))
      .map((asset) => `
        <option value="${escapeHtml(asset.id)}" ${selectedValue === String(asset.id) ? "selected" : ""}>
          ${escapeHtml(asset.name)}${asset.location ? ` - ${escapeHtml(asset.location)}` : ""}
        </option>
      `),
  ].join("");
}

function buildMaintenanceMeterSourceOptions(sourceType = "none", selectedId = "") {
  const selectedValue = String(selectedId || "").trim();
  const normalizedType = String(sourceType || "none").trim().toLowerCase();
  let collection = [];

  if (normalizedType === "engine") {
    collection = state.vessel.engines || [];
  } else if (normalizedType === "generator") {
    collection = state.vessel.generators || [];
  }

  if (!collection.length) {
    return `<option value="">No linked meter</option>`;
  }

  return [
    `<option value="">Select linked meter</option>`,
    ...collection.map((item) => `
      <option value="${escapeHtml(item.id)}" ${selectedValue === String(item.id) ? "selected" : ""}>
        ${escapeHtml(item.label || item.model || item.id)}${Number.isFinite(Number(item.hours)) ? ` - ${escapeHtml(`${item.hours} hrs`)}` : ""}
      </option>
    `),
  ].join("");
}

function renderMaintenanceTemplatePreview(template) {
  if (!elements.maintenanceAssetTemplatePreview) {
    return;
  }

  if (!template) {
    elements.maintenanceAssetTemplatePreview.innerHTML = `
      <div class="maintenance-template-empty">
        <p class="small-copy">Choose a starter template to preview the recurring service items that Harbor Command will install for this vessel.</p>
      </div>
    `;
    return;
  }

  elements.maintenanceAssetTemplatePreview.innerHTML = `
    <div class="maintenance-template-preview-shell">
      <div class="maintenance-template-preview-heading">
        <div>
          <p class="eyebrow">Template preview</p>
          <h4>${escapeHtml(template.name)}</h4>
        </div>
        <span class="maintenance-inline-hint is-good">${template.tasks.length} service items</span>
      </div>
      <p class="small-copy">${escapeHtml(template.description || "Starter recurring service plan.")}</p>
      <div class="maintenance-template-task-list">
        ${template.tasks.map((task) => `
          <article class="maintenance-template-task">
            <div class="maintenance-template-task-topline">
              <strong>${escapeHtml(task.title)}</strong>
              <span class="priority-badge ${priorityClass(task.defaultPriority)}">${escapeHtml(task.defaultPriority)}</span>
            </div>
            <p class="small-copy">${escapeHtml(task.description || "Recurring service task.")}</p>
            <div class="maintenance-template-task-meta">
              <span>${escapeHtml(task.category)}</span>
              <span>${escapeHtml(formatMaintenanceCadence(task))}</span>
            </div>
          </article>
        `).join("")}
      </div>
    </div>
  `;
}

function getMaintenanceAssetTaskCount(assetId) {
  return state.maintenance.filter((task) => String(task.assetId) === String(assetId)).length;
}

function getMaintenanceTemplateTaskCount(templateId) {
  const template = getMaintenanceTemplateById(templateId);
  return template?.tasks?.length || 0;
}

function buildMaintenanceAssetFromForm(formData, existingAsset = null) {
  const selectedTemplateId = String(formData.get("templateId") || "").trim();
  const selectedTemplate = getMaintenanceTemplateById(selectedTemplateId);
  const meterSourceType = String(formData.get("meterSourceType") || "none").trim().toLowerCase() || "none";

  return normalizeMaintenanceAssetItem({
    id: existingAsset?.id || createId("maintenance-asset"),
    templateId: selectedTemplateId,
    name: String(formData.get("name") || "").trim() || selectedTemplate?.name || existingAsset?.name || "Installed asset",
    assetType: String(formData.get("assetType") || "").trim() || selectedTemplate?.assetType || existingAsset?.assetType || "custom-asset",
    manufacturer: String(formData.get("manufacturer") || "").trim(),
    model: String(formData.get("model") || "").trim(),
    serialNumber: String(formData.get("serialNumber") || "").trim(),
    location: String(formData.get("location") || "").trim(),
    meterSourceType,
    meterSourceId: meterSourceType === "engine" || meterSourceType === "generator"
      ? String(formData.get("meterSourceId") || "").trim()
      : "",
    currentHours: normalizePositiveInteger(formData.get("currentHours"), existingAsset?.currentHours || 0, 0),
    notes: String(formData.get("notes") || "").trim(),
    createdAt: existingAsset?.createdAt || currentIsoStamp(),
    updatedAt: currentIsoStamp(),
  });
}

function buildMaintenanceTasksFromTemplate(asset, template) {
  const currentHours = normalizePositiveInteger(asset.currentHours, 0, 0);
  const createdAt = currentIsoStamp();
  return (template?.tasks || []).map((templateTask, index) => normalizeMaintenanceItem({
    id: createId("maintenance-task"),
    title: templateTask.title,
    category: templateTask.category || "General",
    status: "Not Started",
    priority: templateTask.defaultPriority || "Medium",
    assetId: asset.id,
    templateId: template.id,
    templateTaskId: templateTask.id,
    dueDate: usesDateRecurrence(templateTask) && templateTask.intervalDays > 0 ? addDaysToDate(todayStamp(), templateTask.intervalDays) : "",
    dueHours: usesHourRecurrence(templateTask) && templateTask.intervalHours > 0 ? currentHours + templateTask.intervalHours : 0,
    lastCompleted: "",
    lastCompletedHours: 0,
    intervalDays: normalizePositiveInteger(templateTask.intervalDays, 0, 0),
    intervalHours: normalizePositiveInteger(templateTask.intervalHours, 0, 0),
    reminderDays: normalizePositiveInteger(templateTask.reminderDays, 0, 0),
    reminderHours: normalizePositiveInteger(templateTask.reminderHours, 0, 0),
    recurrenceMode: normalizeMaintenanceRecurrenceMode(templateTask.recurrenceMode, templateTask.intervalDays, templateTask.intervalHours),
    meterSourceType: asset.meterSourceType,
    meterSourceId: asset.meterSourceId,
    isCustom: false,
    notes: templateTask.description || "",
    createdAt,
    updatedAt: createdAt,
    sortOrder: state.maintenance.length + index,
  }));
}

function buildStarterPackAssets() {
  const templates = vesselCatalogState.maintenanceTemplates || [];
  const starterAssets = [];

  templates.forEach((template) => {
    if (template.assetType === "main-engine" && Array.isArray(state.vessel.engines) && state.vessel.engines.length) {
      state.vessel.engines.forEach((engine) => {
        starterAssets.push(
          normalizeMaintenanceAssetItem({
            id: createId("maintenance-asset"),
            templateId: template.id,
            name: engine.label || engine.model || `${template.name} ${starterAssets.length + 1}`,
            assetType: template.assetType,
            manufacturer: engine.manufacturer || "",
            model: engine.model || "",
            serialNumber: engine.serialNumber || "",
            location: "Engine room",
            meterSourceType: "engine",
            meterSourceId: engine.id,
            currentHours: normalizePositiveInteger(engine.hours, 0, 0),
            notes: "",
            createdAt: currentIsoStamp(),
            updatedAt: currentIsoStamp(),
          })
        );
      });
      return;
    }

    if (template.assetType === "generator" && Array.isArray(state.vessel.generators) && state.vessel.generators.length) {
      state.vessel.generators.forEach((generator) => {
        starterAssets.push(
          normalizeMaintenanceAssetItem({
            id: createId("maintenance-asset"),
            templateId: template.id,
            name: generator.label || generator.model || `${template.name} ${starterAssets.length + 1}`,
            assetType: template.assetType,
            manufacturer: generator.manufacturer || "",
            model: generator.model || "",
            serialNumber: generator.serialNumber || "",
            location: "Machinery space",
            meterSourceType: "generator",
            meterSourceId: generator.id,
            currentHours: normalizePositiveInteger(generator.hours, 0, 0),
            notes: "",
            createdAt: currentIsoStamp(),
            updatedAt: currentIsoStamp(),
          })
        );
      });
      return;
    }

    starterAssets.push(
      normalizeMaintenanceAssetItem({
        id: createId("maintenance-asset"),
        templateId: template.id,
        name: template.name,
        assetType: template.assetType,
        manufacturer: "",
        model: "",
        serialNumber: "",
        location: "",
        meterSourceType: "none",
        meterSourceId: "",
        currentHours: 0,
        notes: "",
        createdAt: currentIsoStamp(),
        updatedAt: currentIsoStamp(),
      })
    );
  });

  return starterAssets;
}

function installStarterMaintenancePack() {
  const existingAssetKeys = new Set(
    state.maintenanceAssets.map((asset) => `${asset.templateId}::${asset.meterSourceType}::${asset.meterSourceId}::${asset.name}`)
  );
  const starterAssets = buildStarterPackAssets().filter((asset) => {
    const key = `${asset.templateId}::${asset.meterSourceType}::${asset.meterSourceId}::${asset.name}`;
    return !existingAssetKeys.has(key);
  });

  if (!starterAssets.length) {
    window.alert("The starter maintenance pack is already installed for this vessel.");
    return;
  }

  const starterTasks = starterAssets.flatMap((asset) => {
    const template = getMaintenanceTemplateById(asset.templateId);
    return template ? buildMaintenanceTasksFromTemplate(asset, template) : [];
  });

  state.maintenanceAssets = normalizeMaintenanceAssetCollection([...starterAssets, ...state.maintenanceAssets], []);
  state.maintenance = normalizeMaintenanceCollection([...starterTasks, ...state.maintenance], []);
  editingMaintenanceAssetId = null;
  state.activeView = "maintenance";
  state.activeMaintenanceWorkspace = "systems";
  persistAndRender();
}

function scrollToMaintenanceAssetBuilder() {
  activateMaintenanceWorkspace("systems", {
    focusSelector: "#maintenance-asset-form",
  });
}

function scrollToMaintenanceTaskEditor() {
  resetMaintenanceForm();
  activateMaintenanceWorkspace("new-service-task", {
    focusSelector: "#maintenance-form",
  });
}

function handleMaintenanceQuickAction(action) {
  if (isOwnerReadOnly()) {
    return;
  }

  if (action === "starter-pack") {
    installStarterMaintenancePack();
    return;
  }

  if (action === "add-equipment") {
    resetMaintenanceAssetForm();
    scrollToMaintenanceAssetBuilder();
    return;
  }

  if (action === "start-scratch") {
    scrollToMaintenanceTaskEditor();
  }
}

function getMaintenanceHistoryTaskLabel(entry) {
  const linkedTask = state.maintenance.find((task) => String(task.id) === String(entry.maintenanceLogId));
  if (linkedTask) {
    return linkedTask.title;
  }

  const linkedAsset = getMaintenanceAssetById(entry.assetId);
  if (linkedAsset) {
    return linkedAsset.name;
  }

  return "Maintenance task";
}

function usesDateRecurrence(task) {
  return ["days", "days-or-hours"].includes(String(task?.recurrenceMode || "days").trim().toLowerCase());
}

function usesHourRecurrence(task) {
  return ["hours", "days-or-hours"].includes(String(task?.recurrenceMode || "days").trim().toLowerCase());
}

function getLinkedMaintenanceTask(order, targetState = state) {
  if (!order?.maintenanceLogId) {
    return null;
  }

  return (targetState.maintenance || []).find((task) => String(task.id) === String(order.maintenanceLogId)) || null;
}

function getLinkedWorkOrderForMaintenance(task, targetState = state) {
  if (!task?.id) {
    return null;
  }

  return (targetState.workOrders || []).find((order) =>
    String(order.maintenanceLogId) === String(task.id) && !isWorkOrderComplete(order)
  ) || null;
}

function hasWorkOrderNarrative(order) {
  return Boolean(
    String(order?.workDone || "").trim()
    || String(order?.systemsChecked || "").trim()
    || String(order?.issues || "").trim()
    || String(order?.notes || "").trim()
  );
}

function isMaintenanceSuggestionActiveForWeek(task, weekRange = getActiveWorkWeekRange()) {
  if (usesDateRecurrence(task) && task?.dueDate) {
    const reminderStart = addDaysToDate(task.dueDate, -getMaintenanceReminderDays(task));
    if (reminderStart <= weekRange.end) {
      return true;
    }
  }

  if (usesHourRecurrence(task)) {
    const hoursRemaining = getMaintenanceHoursRemaining(task);
    if (hoursRemaining !== null && hoursRemaining <= getMaintenanceReminderHours(task)) {
      return true;
    }
  }

  return false;
}

function clampDateToWeek(value, weekRange = getActiveWorkWeekRange()) {
  const safeValue = formatInputDate(value || weekRange.start);
  if (safeValue < weekRange.start) {
    return weekRange.start;
  }
  if (safeValue > weekRange.end) {
    return weekRange.end;
  }
  return safeValue;
}

function buildSuggestedWorkOrder(task, weekRange = getActiveWorkWeekRange()) {
  const timestamp = currentIsoStamp();
  return normalizeWorkOrderItem({
    id: `maintenance-suggestion-${task.id}-${weekRange.start}`,
    item: task.title,
    reportDate: clampDateToWeek(task.dueDate || weekRange.start, weekRange),
    workDone: "",
    systemsChecked: "",
    issues: "",
    notes: task.notes || "Suggested from recurring maintenance.",
    weekStart: weekRange.start,
    weekEnd: weekRange.end,
    status: "Open",
    priority: task.priority || "Medium",
    maintenanceLogId: task.id,
    originType: "maintenance-suggestion",
    completedAt: "",
    createdAt: timestamp,
    updatedAt: timestamp,
  });
}

function syncConnectedAutomation(targetState = state) {
  if (!targetState || typeof targetState !== "object") {
    return targetState;
  }

  const weekRange = getWorkWeekRange(targetState.activeWorkWeekStart || todayStamp());
  const maintenanceAssets = normalizeMaintenanceAssetCollection(targetState.maintenanceAssets, []);
  const maintenance = normalizeMaintenanceCollection(targetState.maintenance, defaultState.maintenance);
  let maintenanceHistory = normalizeMaintenanceHistoryCollection(targetState.maintenanceHistory, []);
  let workOrders = normalizeWorkOrderCollection(targetState.workOrders, []);
  const maintenanceById = new Map(maintenance.map((task) => [String(task.id), task]));

  workOrders.forEach((order) => {
    if (!order.maintenanceLogId || !isWorkOrderComplete(order)) {
      return;
    }

    const linkedTask = maintenanceById.get(String(order.maintenanceLogId));
    if (!linkedTask) {
      return;
    }

    const completedOn = formatInputDate(order.reportDate || order.completedAt || todayStamp());
    order.completedAt = order.completedAt || currentIsoStamp();
    linkedTask.status = "Completed";
    if (usesDateRecurrence(linkedTask)) {
      linkedTask.lastCompleted = completedOn;
      linkedTask.dueDate = linkedTask.intervalDays > 0 ? addDaysToDate(completedOn, getMaintenanceIntervalDays(linkedTask)) : "";
    }
    const completedHours = usesHourRecurrence(linkedTask) ? getMaintenanceCurrentHours(linkedTask, targetState) : 0;
    if (usesHourRecurrence(linkedTask)) {
      linkedTask.lastCompletedHours = completedHours;
      linkedTask.dueHours = linkedTask.intervalHours > 0 ? completedHours + getMaintenanceIntervalHours(linkedTask) : 0;
    }

    const historyId = `maintenance-history-${order.id}`;
    const existingHistoryIndex = maintenanceHistory.findIndex((entry) => entry.id === historyId);
    const nextHistoryEntry = normalizeMaintenanceHistoryEntry({
      id: historyId,
      vesselId: targetState.activeVesselId,
      maintenanceLogId: linkedTask.id,
      assetId: linkedTask.assetId,
      templateTaskId: linkedTask.templateTaskId,
      workOrderId: order.id,
      source: "work-order",
      completedAt: order.completedAt,
      completionDate: completedOn,
      completedHours,
      workDone: order.workDone,
      systemsChecked: order.systemsChecked,
      issues: order.issues,
      notes: order.notes,
      createdAt: order.createdAt || currentIsoStamp(),
      updatedAt: order.updatedAt || currentIsoStamp(),
    });
    if (existingHistoryIndex >= 0) {
      maintenanceHistory[existingHistoryIndex] = nextHistoryEntry;
    } else {
      maintenanceHistory.unshift(nextHistoryEntry);
    }
  });

  workOrders = workOrders.filter((order) => {
    if (order.originType !== "maintenance-suggestion" || isWorkOrderComplete(order)) {
      return true;
    }

    const linkedTask = maintenanceById.get(String(order.maintenanceLogId));
    if (!linkedTask) {
      return hasWorkOrderNarrative(order);
    }

    if (isMaintenanceSuggestionActiveForWeek(linkedTask, weekRange)) {
      return true;
    }

    return hasWorkOrderNarrative(order);
  });

  maintenance.forEach((task) => {
    if (!isMaintenanceSuggestionActiveForWeek(task, weekRange)) {
      return;
    }

    const existingLinkedOrder = workOrders.find((order) =>
      String(order.maintenanceLogId) === String(task.id)
      && order.weekStart === weekRange.start
      && order.weekEnd === weekRange.end
      && !isWorkOrderComplete(order)
    );

    if (!existingLinkedOrder) {
      workOrders.unshift(buildSuggestedWorkOrder(task, weekRange));
    }
  });

  targetState.maintenance = maintenance;
  targetState.maintenanceAssets = maintenanceAssets;
  targetState.maintenanceHistory = normalizeMaintenanceHistoryCollection(maintenanceHistory, []);
  targetState.workOrders = workOrders
    .sort((left, right) => left.reportDate.localeCompare(right.reportDate) || left.sortOrder - right.sortOrder)
    .map((order, index) => ({
      ...order,
      sortOrder: index,
    }));

  return targetState;
}

function extractLegacyWorkOrders(workOrders, maintenance, fallback) {
  if (Array.isArray(workOrders)) {
    return workOrders;
  }

  return cloneItems(fallback);
}

function currentIsoStamp() {
  return new Date().toISOString();
}

function normalizeReportEntry(item, index = 0) {
  if (!item || typeof item !== "object") {
    return {
      id: createId("report-entry"),
      item: "General update",
      reportDate: todayStamp(),
      workDone: "",
      systemsChecked: "",
      issues: "",
      notes: "",
      createdAt: currentIsoStamp(),
      updatedAt: currentIsoStamp(),
      sortOrder: index,
    };
  }

  return {
    ...item,
    id: String(item.id || createId("report-entry")),
    item: item.item || "General update",
    reportDate: formatInputDate(item.reportDate || item.date || todayStamp()),
    workDone: item.workDone || item.summary || "",
    systemsChecked: item.systemsChecked || "",
    issues: item.issues || "",
    notes: item.notes || "",
    createdAt: item.createdAt || currentIsoStamp(),
    updatedAt: item.updatedAt || currentIsoStamp(),
    sortOrder: Number.isFinite(Number(item.sortOrder)) ? Number(item.sortOrder) : index,
  };
}

function normalizeWeeklyReport(item, index = 0) {
  const normalizedEntries = Array.isArray(item?.entries)
    ? item.entries.map((entry, entryIndex) => normalizeReportEntry(entry, entryIndex))
    : [];
  const sourceDate = item?.weekStart || normalizedEntries[0]?.reportDate || todayStamp();
  const weekRange = getWorkWeekRange(sourceDate);

  return {
    id: String(item?.id || createId("weekly-report")),
    weekStart: formatInputDate(item?.weekStart || weekRange.start),
    weekEnd: formatInputDate(item?.weekEnd || weekRange.end),
    status: String(item?.status || "draft").toLowerCase() === "finalized" ? "finalized" : "draft",
    createdAt: item?.createdAt || currentIsoStamp(),
    updatedAt: item?.updatedAt || currentIsoStamp(),
    sortOrder: Number.isFinite(Number(item?.sortOrder)) ? Number(item.sortOrder) : index,
    vesselSnapshot: item?.vesselSnapshot && typeof item.vesselSnapshot === "object"
      ? { ...item.vesselSnapshot }
      : null,
    entries: normalizedEntries
      .sort((left, right) => parseDateValue(left.reportDate) - parseDateValue(right.reportDate))
      .map((entry, entryIndex) => ({
        ...entry,
        sortOrder: entryIndex,
      })),
  };
}

function buildWeeklyReportsFromLegacyRows(items) {
  const grouped = new Map();
  cloneItems(items).forEach((item, index) => {
    const entry = normalizeReportEntry(item, index);
    const weekRange = getWorkWeekRange(entry.reportDate);
    const reportKey = `${weekRange.start}:${weekRange.end}`;
    const existingReport = grouped.get(reportKey);

    if (existingReport) {
      existingReport.entries.push({
        ...entry,
        sortOrder: existingReport.entries.length,
      });
      existingReport.updatedAt = entry.updatedAt || existingReport.updatedAt;
      return;
    }

    grouped.set(reportKey, {
      id: `weekly-report-${weekRange.start}`,
      weekStart: weekRange.start,
      weekEnd: weekRange.end,
      status: compareDateStrings(weekRange.end, todayStamp()) < 0 ? "finalized" : "draft",
      createdAt: entry.createdAt || currentIsoStamp(),
      updatedAt: entry.updatedAt || currentIsoStamp(),
      sortOrder: grouped.size,
      entries: [
        {
          ...entry,
          sortOrder: 0,
        },
      ],
    });
  });

  return Array.from(grouped.values())
    .sort((left, right) => right.weekStart.localeCompare(left.weekStart))
    .map((report, reportIndex) => ({
      ...report,
      sortOrder: reportIndex,
      entries: report.entries
        .slice()
        .sort((left, right) => parseDateValue(left.reportDate) - parseDateValue(right.reportDate))
        .map((entry, entryIndex) => ({
          ...entry,
          sortOrder: entryIndex,
        })),
    }));
}

function normalizeReportCollection(items, fallback) {
  const source = Array.isArray(items) ? items : cloneItems(fallback);
  if (!source.length) {
    return [];
  }

  const looksLikeWeeklyReports = source.some((item) =>
    Array.isArray(item?.entries)
    || Object.prototype.hasOwnProperty.call(item || {}, "weekStart")
    || Object.prototype.hasOwnProperty.call(item || {}, "weekEnd")
    || Object.prototype.hasOwnProperty.call(item || {}, "status")
  );

  if (!looksLikeWeeklyReports) {
    return buildWeeklyReportsFromLegacyRows(source);
  }

  return source
    .map((item, index) => normalizeWeeklyReport(item, index))
    .sort((left, right) => right.weekStart.localeCompare(left.weekStart))
    .map((report, reportIndex) => ({
      ...report,
      sortOrder: reportIndex,
      entries: report.entries.map((entry, entryIndex) => ({
        ...entry,
        sortOrder: entryIndex,
      })),
    }));
}

function hydrateState() {
  if (!VIEW_OPTIONS.includes(state.activeView)) {
    state.activeView = "overview";
  }

  state.activeWorkWeekStart = normalizeActiveWorkWeekStart(state.activeWorkWeekStart);

  ensureAccessibleView();

  if (!MAINTENANCE_FILTERS.includes(state.activeMaintenanceFilter)) {
    state.activeMaintenanceFilter = "all";
  }

  if (!MAINTENANCE_WORKSPACE_VIEWS.includes(state.activeMaintenanceWorkspace)) {
    state.activeMaintenanceWorkspace = "overview";
  }

  const validMaintenanceCategories = new Set(["all", ...getMaintenanceCategoryOptions(state.maintenance)]);
  if (!validMaintenanceCategories.has(state.activeMaintenanceCategory)) {
    state.activeMaintenanceCategory = getDefaultMaintenanceCategory(state.maintenance);
  }

  if (typeof state.activeMaintenanceQuery !== "string") {
    state.activeMaintenanceQuery = "";
  }

  if (!WORK_ORDER_SORT_OPTIONS.includes(state.activeWorkOrderSort)) {
    state.activeWorkOrderSort = "date-asc";
  }

  if (!MAINTENANCE_SORT_OPTIONS.includes(state.activeMaintenanceSort)) {
    state.activeMaintenanceSort = "category";
  }

  if (!VENDOR_FILTERS.includes(state.activeVendorFilter)) {
    state.activeVendorFilter = "all";
  }

  if (!VENDOR_SORT_OPTIONS.includes(state.activeVendorSort)) {
    state.activeVendorSort = "name-asc";
  }

  if (!INVENTORY_SORT_OPTIONS.includes(state.activeInventorySort)) {
    state.activeInventorySort = "name-asc";
  }

  if (!EXPENSE_SORT_OPTIONS.includes(state.activeExpenseSort)) {
    state.activeExpenseSort = "date-desc";
  }

  if (!CHARTER_SORT_OPTIONS.includes(state.activeCharterSort)) {
    state.activeCharterSort = "start-asc";
  }

  if (!VOYAGE_SORT_OPTIONS.includes(state.activeVoyageSort)) {
    state.activeVoyageSort = "departure-asc";
  }

  syncConnectedAutomation(state);
  ensureActiveReportSelection();
}

function scrollActiveViewIntoView(behavior = "smooth") {
  const activePanel = elements.viewPanels.find((panel) => panel.dataset.viewPanel === state.activeView);
  if (!activePanel) {
    return;
  }

  window.requestAnimationFrame(() => {
    activePanel.scrollIntoView({ behavior, block: "start" });
  });
}

function activateWorkspaceView(nextView, options = {}) {
  const { persist = true, behavior = "smooth" } = options;

  if (!VIEW_OPTIONS.includes(nextView)) {
    return;
  }

  state.activeView = nextView;

  if (persist) {
    persistAndRender();
  } else {
    renderApp();
  }

  scrollActiveViewIntoView(behavior);
}

function bindEvents() {
  elements.authForm.addEventListener("submit", handleAuthSubmit);
  elements.authToggle.addEventListener("click", handleAuthToggle);
  if (elements.authRetry) {
    elements.authRetry.addEventListener("click", () => {
      void bootstrapAuth();
    });
  }
  elements.logoutButton.addEventListener("click", handleLogout);
  elements.vesselForm.addEventListener("submit", handleVesselSubmit);
  elements.vesselSystemsForm.addEventListener("submit", handleVesselSystemsSubmit);
  elements.vesselAddForm.addEventListener("submit", handleCatalogVesselCreate);
  elements.vesselAddForm.addEventListener("input", handleVesselCatalogSelectionChange);
  elements.vesselAddForm.addEventListener("change", handleVesselCatalogSelectionChange);
  elements.vesselAddPhotoInput.addEventListener("change", handleVesselAddPhotoChange);
  elements.clearVesselAddPhoto.addEventListener("click", clearVesselAddPhoto);
  elements.createCustomVesselButton.addEventListener("click", handleCreateCustomVessel);
  elements.vesselPhotoInput.addEventListener("change", handleVesselPhotoChange);
  elements.clearVesselPhoto.addEventListener("click", clearVesselPhoto);
  elements.engineForm.addEventListener("submit", handleEngineSubmit);
  elements.engineCancel.addEventListener("click", resetEngineForm);
  elements.generatorForm.addEventListener("submit", handleGeneratorSubmit);
  elements.generatorCancel.addEventListener("click", resetGeneratorForm);
  elements.maintenanceAssetForm.addEventListener("submit", handleMaintenanceAssetSubmit);
  elements.maintenanceAssetForm.addEventListener("change", handleMaintenanceAssetFormChange);
  elements.maintenanceAssetCancel.addEventListener("click", resetMaintenanceAssetForm);
  elements.maintenanceForm.addEventListener("submit", handleMaintenanceSubmit);
  elements.maintenanceForm.addEventListener("change", handleMaintenanceTaskFormChange);
  elements.maintenanceCancel.addEventListener("click", resetMaintenanceForm);
  elements.workOrderForm.addEventListener("submit", handleWorkOrderSubmit);
  elements.workOrderCancel.addEventListener("click", resetWorkOrderForm);
  elements.charterForm.addEventListener("submit", handleCharterSubmit);
  elements.reportHistorySelect.addEventListener("change", handleReportHistoryChange);
  elements.reportViewButton.addEventListener("click", () => {
    if (!getSelectedWeeklyReport()) {
      return;
    }
    elements.reportPreviewSection?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
  elements.reportExportPdf.addEventListener("click", handleExportWeeklyReportPdf);
  elements.reportToggleStatus.addEventListener("click", () => {
    void handleToggleWeeklyReportStatus();
  });
  elements.workOrderGenerateReport.addEventListener("click", () => {
    void handleGenerateWeeklyReportFromWorkspace();
  });
  elements.workOrderOpenReports.addEventListener("click", () => {
    state.activeView = "reports";
    renderApp();
    scrollActiveViewIntoView("smooth");
  });
  elements.vendorForm.addEventListener("submit", handleVendorSubmit);
  elements.vendorCancel.addEventListener("click", resetVendorForm);
  elements.inventoryForm.addEventListener("submit", handleInventorySubmit);
  elements.inventoryCancel.addEventListener("click", resetInventoryForm);
  elements.expenseForm.addEventListener("submit", handleExpenseSubmit);
  elements.expenseCancel.addEventListener("click", resetExpenseForm);
  elements.expenseForm.elements.namedItem("vendor").addEventListener("change", handleExpenseVendorChange);
  elements.inviteForm.addEventListener("submit", handleInviteSubmit);
  elements.inviteForm.addEventListener("change", (event) => {
    if (event.target?.name === "role") {
      renderAccessModal();
    }
  });
  elements.openAccessModal.addEventListener("click", () => {
    void openAccessModal();
  });
  elements.closeAccessModal.addEventListener("click", closeAccessModal);
  elements.accessModalBackdrop.addEventListener("click", closeAccessModal);

  elements.workspaceNav.addEventListener("click", (event) => {
    const tab = event.target.closest("[data-view]");
    if (!tab) {
      return;
    }

    activateWorkspaceView(tab.dataset.view);
  });

  elements.maintenanceFilters.addEventListener("click", (event) => {
    const filterButton = event.target.closest("[data-maintenance-filter]");
    if (!filterButton) {
      return;
    }

    state.activeMaintenanceFilter = filterButton.dataset.maintenanceFilter;
    persistAndRender();
  });

  elements.maintenanceSearch.addEventListener("input", (event) => {
    state.activeMaintenanceQuery = event.currentTarget.value;
    renderMaintenance();
  });

  elements.maintenanceSort.addEventListener("change", (event) => {
    state.activeMaintenanceSort = event.currentTarget.value;
    persistAndRender();
  });

  elements.maintenanceSubnav?.addEventListener("click", (event) => {
    const sectionButton = event.target.closest("[data-maintenance-view]");
    if (!sectionButton) {
      return;
    }

    activateMaintenanceWorkspace(sectionButton.dataset.maintenanceView);
  });

  elements.maintenanceSections.addEventListener("click", (event) => {
    const sectionButton = event.target.closest("[data-maintenance-category]");
    if (!sectionButton) {
      return;
    }

    state.activeMaintenanceCategory = sectionButton.dataset.maintenanceCategory;
    renderMaintenance();
    renderMaintenanceForm();
  });

  elements.overviewPanel.addEventListener("click", (event) => {
    const target = event.target.closest("[data-view-target]");
    if (!target) {
      return;
    }

    if (target.dataset.reportId) {
      state.activeReportId = target.dataset.reportId;
    }

    activateWorkspaceView(target.dataset.viewTarget);
  });

  elements.spotlightPanel.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-weather-action]");
    if (!actionButton) {
      return;
    }

    ensureWeatherData({ force: true });
  });

  elements.vesselDashboard.addEventListener("click", (event) => {
    const selectorButton = event.target.closest("[data-vessel-select]");
    if (selectorButton) {
      activateRuntimeVesselState(state, selectorButton.dataset.vesselSelect);
      state.activeView = "vessel";
      persistAndRender();
      return;
    }

    const scrollButton = event.target.closest("[data-vessel-scroll]");
    if (scrollButton) {
      const target = document.querySelector(scrollButton.dataset.vesselScroll);
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
      return;
    }

    const addButton = event.target.closest("[data-vessel-action='add']");
    if (addButton) {
      document.querySelector("#vessel-add-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }

    const deleteButton = event.target.closest("[data-vessel-action='delete']");
    if (deleteButton) {
      handleDeleteActiveVessel();
      return;
    }
  });

  elements.engineList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-engine-action]");
    if (!actionButton) {
      return;
    }

    const engine = state.vessel.engines.find((item) => item.id === actionButton.dataset.engineId);
    if (!engine) {
      return;
    }

    if (actionButton.dataset.engineAction === "delete") {
      state.vessel.engines = state.vessel.engines.filter((item) => item.id !== engine.id);
      state.vessel.engineInfo = buildMachinerySummary(state.vessel.engines, "");
      if (editingEngineId === engine.id) {
        resetEngineForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.engineAction === "edit") {
      editingEngineId = engine.id;
      renderApp();
      elements.engineForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });

  elements.generatorList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-generator-action]");
    if (!actionButton) {
      return;
    }

    const generator = state.vessel.generators.find((item) => item.id === actionButton.dataset.generatorId);
    if (!generator) {
      return;
    }

    if (actionButton.dataset.generatorAction === "delete") {
      state.vessel.generators = state.vessel.generators.filter((item) => item.id !== generator.id);
      state.vessel.generatorInfo = buildMachinerySummary(state.vessel.generators, "");
      if (editingGeneratorId === generator.id) {
        resetGeneratorForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.generatorAction === "edit") {
      editingGeneratorId = generator.id;
      renderApp();
      elements.generatorForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });

  elements.maintenanceList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-task-action]");
    if (!actionButton) {
      return;
    }

    if (isOwnerReadOnly()) {
      return;
    }

    const task = state.maintenance.find((item) => item.id === actionButton.dataset.taskId);
    if (!task) {
      return;
    }

    if (actionButton.dataset.taskAction === "delete") {
      const shouldDelete = window.confirm(`Delete ${task.title} from maintenance?`);
      if (!shouldDelete) {
        return;
      }

      state.maintenance = state.maintenance.filter((item) => item.id !== task.id);
      if (editingMaintenanceId === task.id) {
        resetMaintenanceForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.taskAction === "toggle") {
      if (isMaintenanceComplete(task)) {
        task.status = "Not Started";
      } else {
        completeMaintenanceTask(task);
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.taskAction === "edit") {
      editingMaintenanceId = task.id;
      activateMaintenanceWorkspace("new-service-task", {
        focusSelector: "#maintenance-form",
      });
      return;
    }

    if (actionButton.dataset.taskAction === "open-work-order") {
      const linkedOrder = state.workOrders.find((item) => item.id === actionButton.dataset.workOrderId);
      if (!linkedOrder) {
        return;
      }
      editingWorkOrderId = linkedOrder.id;
      state.activeView = "work-orders";
      renderApp();
      elements.workOrderForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });

  elements.maintenanceAssetsList.addEventListener("click", (event) => {
    const quickActionButton = event.target.closest("[data-maintenance-quick-action]");
    if (quickActionButton) {
      handleMaintenanceQuickAction(quickActionButton.dataset.maintenanceQuickAction);
      return;
    }

    const actionButton = event.target.closest("[data-maintenance-asset-action]");
    if (!actionButton) {
      return;
    }

    if (isOwnerReadOnly()) {
      return;
    }

    const asset = state.maintenanceAssets.find((item) => String(item.id) === String(actionButton.dataset.assetId));
    if (!asset) {
      return;
    }

    if (actionButton.dataset.maintenanceAssetAction === "edit") {
      editingMaintenanceAssetId = asset.id;
      activateMaintenanceWorkspace("systems", {
        focusSelector: "#maintenance-asset-form",
      });
      return;
    }

    if (actionButton.dataset.maintenanceAssetAction === "delete") {
      const linkedTaskIds = new Set(
        state.maintenance.filter((task) => String(task.assetId) === String(asset.id)).map((task) => String(task.id))
      );
      const confirmed = window.confirm(`Delete ${asset.name} and its linked recurring maintenance tasks?`);
      if (!confirmed) {
        return;
      }

      state.maintenanceAssets = state.maintenanceAssets.filter((item) => String(item.id) !== String(asset.id));
      state.maintenance = state.maintenance.filter((task) => String(task.assetId) !== String(asset.id));
      state.maintenanceHistory = state.maintenanceHistory.filter((entry) => String(entry.assetId) !== String(asset.id));
      state.workOrders = state.workOrders.map((order) => linkedTaskIds.has(String(order.maintenanceLogId))
        ? { ...order, maintenanceLogId: "" }
        : order
      );
      if (editingMaintenanceAssetId === asset.id) {
        resetMaintenanceAssetForm();
      }
      persistAndRender();
    }
  });

  elements.maintenanceOverviewSpotlight?.addEventListener("click", (event) => {
    const quickActionButton = event.target.closest("[data-maintenance-quick-action]");
    if (!quickActionButton) {
      return;
    }

    handleMaintenanceQuickAction(quickActionButton.dataset.maintenanceQuickAction);
  });

  elements.workOrderList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-work-order-action]");
    if (!actionButton) {
      return;
    }

    const order = state.workOrders.find((item) => item.id === actionButton.dataset.workOrderId);
    if (!order) {
      return;
    }

    if (actionButton.dataset.workOrderAction === "delete") {
      const shouldDelete = window.confirm(`Delete ${order.item} from this week's workspace?`);
      if (!shouldDelete) {
        return;
      }

      state.workOrders = state.workOrders.filter((item) => item.id !== order.id);
      if (editingWorkOrderId === order.id) {
        resetWorkOrderForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.workOrderAction === "edit") {
      editingWorkOrderId = order.id;
      state.activeView = "work-orders";
      renderApp();
      elements.workOrderForm.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }

    if (actionButton.dataset.workOrderAction === "toggle") {
      if (isWorkOrderComplete(order)) {
        order.status = "Open";
        order.completedAt = "";
      } else {
        order.status = "Completed";
        order.completedAt = currentIsoStamp();
      }
      persistAndRender();
    }
  });

  elements.vendorFilters.addEventListener("click", (event) => {
    const filterButton = event.target.closest("[data-vendor-filter]");
    if (!filterButton) {
      return;
    }

    state.activeVendorFilter = filterButton.dataset.vendorFilter;
    persistAndRender();
  });

  elements.workOrderSort.addEventListener("change", (event) => {
    state.activeWorkOrderSort = event.currentTarget.value;
    persistAndRender();
  });

  elements.charterSort.addEventListener("change", (event) => {
    state.activeCharterSort = event.currentTarget.value;
    persistAndRender();
  });

  elements.vendorSort.addEventListener("change", (event) => {
    state.activeVendorSort = event.currentTarget.value;
    persistAndRender();
  });

  elements.inventorySort.addEventListener("change", (event) => {
    state.activeInventorySort = event.currentTarget.value;
    persistAndRender();
  });

  elements.expenseSort.addEventListener("change", (event) => {
    state.activeExpenseSort = event.currentTarget.value;
    persistAndRender();
  });

  elements.voyageSort.addEventListener("change", (event) => {
    state.activeVoyageSort = event.currentTarget.value;
    persistAndRender();
  });

  elements.vendorTable.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-vendor-action]");
    if (!actionButton) {
      return;
    }

    const vendor = state.vendors.find((item) => item.id === actionButton.dataset.vendorId);
    if (!vendor) {
      return;
    }

    if (actionButton.dataset.vendorAction === "delete") {
      const shouldDelete = window.confirm(`Delete ${vendor.name} from vendors?`);
      if (!shouldDelete) {
        return;
      }

      state.vendors = state.vendors.filter((item) => item.id !== vendor.id);
      if (editingVendorId === vendor.id) {
        resetVendorForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.vendorAction === "edit") {
      editingVendorId = vendor.id;
      state.activeView = "vendors";
      renderApp();
      elements.vendorForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });

  elements.inventoryList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-inventory-action]");
    if (!actionButton) {
      return;
    }

    const item = state.inventory.find((entry) => entry.id === actionButton.dataset.inventoryId);
    if (!item) {
      return;
    }

    if (actionButton.dataset.inventoryAction === "delete") {
      const shouldDelete = window.confirm(`Delete ${item.name} from inventory?`);
      if (!shouldDelete) {
        return;
      }

      state.inventory = state.inventory.filter((entry) => entry.id !== item.id);
      if (editingInventoryId === item.id) {
        resetInventoryForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.inventoryAction === "edit") {
      editingInventoryId = item.id;
      state.activeView = "inventory";
      renderApp();
      elements.inventoryForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });

  elements.expensesList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-expense-action]");
    if (!actionButton) {
      return;
    }

    const expense = state.expenses.find((entry) => entry.id === actionButton.dataset.expenseId);
    if (!expense) {
      return;
    }

    if (actionButton.dataset.expenseAction === "delete") {
      const shouldDelete = window.confirm(`Delete ${expense.title} from expenses?`);
      if (!shouldDelete) {
        return;
      }

      state.expenses = state.expenses.filter((entry) => entry.id !== expense.id);
      if (editingExpenseId === expense.id) {
        resetExpenseForm();
      }
      persistAndRender();
      return;
    }

    if (actionButton.dataset.expenseAction === "edit") {
      editingExpenseId = expense.id;
      state.activeView = "expenses";
      renderApp();
      elements.expenseForm.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });

  elements.usersList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-user-action]");
    if (!actionButton) {
      return;
    }

    void handleManagedUserAction(actionButton);
  });

  elements.inviteList.addEventListener("click", (event) => {
    const actionButton = event.target.closest("[data-invite-action]");
    if (!actionButton) {
      return;
    }

    void handleInviteAction(actionButton);
  });

  elements.jumpToVessel.addEventListener("click", () => {
    state.activeView = "vessel";
    renderApp();
    elements.vesselDashboard.scrollIntoView({ behavior: "smooth", block: "start" });
  });

  elements.jumpToMaintenanceForm.addEventListener("click", () => {
    if (isOwnerReadOnly()) {
      return;
    }
    scrollToMaintenanceTaskEditor();
  });

  elements.jumpToMaintenanceAssetForm?.addEventListener("click", () => {
    if (isOwnerReadOnly()) {
      return;
    }
    resetMaintenanceAssetForm();
    scrollToMaintenanceAssetBuilder();
  });

  elements.maintenanceApplyStarterPack?.addEventListener("click", () => {
    if (isOwnerReadOnly()) {
      return;
    }
    installStarterMaintenancePack();
  });

  elements.jumpToWorkOrderForm.addEventListener("click", () => {
    resetWorkOrderForm();
    state.activeView = "work-orders";
    renderApp();
    elements.workOrderForm.scrollIntoView({ behavior: "smooth", block: "start" });
  });

  elements.jumpToVendorForm.addEventListener("click", () => {
    resetVendorForm();
    state.activeView = "vendors";
    renderApp();
    elements.vendorForm.scrollIntoView({ behavior: "smooth", block: "start" });
  });

  elements.jumpToInventoryForm.addEventListener("click", () => {
    resetInventoryForm();
    state.activeView = "inventory";
    renderApp();
    elements.inventoryForm.scrollIntoView({ behavior: "smooth", block: "start" });
  });

  elements.jumpToExpenseForm.addEventListener("click", () => {
    resetExpenseForm();
    state.activeView = "expenses";
    renderApp();
    elements.expenseForm.scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

function clearAuthForm() {
  elements.authForm.reset();
  elements.authEmailInput.readOnly = false;
  elements.authRoleSelect.disabled = false;
  elements.authPasswordInput.autocomplete = "current-password";
  elements.authPasswordLabel.textContent = "Password";
}

function handleAuthToggle() {
  if (isInviteMode()) {
    clearInviteState();
    clearInviteTokenFromUrl();
    clearAuthForm();
    void bootstrapAuth();
    return;
  }

  if (authState.hasUsers) {
    setAuthState({
      mode: "login",
      error: "",
    });
    renderAuthShell();
    return;
  }

  setAuthState({
    mode: isSetupMode() ? "login" : "setup",
    error: "",
  });
  renderAuthShell();
}

async function handleAuthSubmit(event) {
  event.preventDefault();

  if (isFileModeApp()) {
    const redirected = await redirectToLiveHarborCommand();
    if (redirected) {
      return;
    }

    setAuthState({
      pending: false,
      error: AUTH_SERVICE_UNAVAILABLE_MESSAGE,
    });
    renderAuthShell();
    return;
  }

  const inviteMode = isInviteMode();
  const setupMode = isSetupMode();
  const formData = new FormData(event.currentTarget);
  const payload = inviteMode
    ? {
        name: String(formData.get("name")).trim(),
        password: String(formData.get("password")),
      }
    : setupMode
    ? {
        name: String(formData.get("name")).trim(),
        email: String(formData.get("email")).trim(),
        password: String(formData.get("password")),
        role: String(formData.get("role")),
      }
    : {
        email: String(formData.get("email")).trim(),
        password: String(formData.get("password")),
      };

  setAuthState({
    pending: true,
    error: "",
  });
  setInviteState({
    pending: inviteMode,
    error: "",
  });
  renderAuthShell();

  try {
    const endpoint = inviteMode ? `/invite/${encodeURIComponent(inviteState.token)}/accept` : `/auth/${setupMode ? "setup" : "login"}`;
    const response = await fetchFromHarborApi(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();

    if (!response.ok) {
      throw new Error(result?.error || "Unable to complete authentication.");
    }

    setAuthState({
      authenticated: true,
      hasUsers: true,
      user: result.user || null,
      mode: "authenticated",
      error: "",
    });
    if (inviteMode) {
      clearInviteTokenFromUrl();
      clearInviteState();
    }
    clearAuthForm();
    renderApp();
    await bootstrapApiState();
  } catch (error) {
    const errorMessage = getFriendlyAuthError(error);
    if (inviteMode) {
      setInviteState({
        pending: false,
        error: errorMessage,
      });
      setAuthState({
        authenticated: false,
        pending: false,
        error: "",
      });
    } else {
      setAuthState({
        authenticated: false,
        error: errorMessage,
        mode: setupMode ? "setup" : "login",
      });
    }
  } finally {
    setAuthState({ pending: false });
    setInviteState({ pending: false });
    renderAuthShell();
  }
}

async function handleLogout() {
  try {
    await fetchFromHarborApi("/auth/logout", {
      method: "POST",
    });
  } catch (error) {
    console.warn("Unable to reach logout endpoint.", error);
  }

  if (apiSyncTimer) {
    clearTimeout(apiSyncTimer);
    apiSyncTimer = null;
  }

  apiPersistQueued = false;
  apiPersistInFlight = false;
  pendingStateSnapshot = null;
  clearCachedState();
  clearPendingUiScrollState();
  clearManagedUsersState();
  clearManagedInvitesState();
  closeAccessModal();
  setAuthState({
    authenticated: false,
    user: null,
    mode: authState.hasUsers ? "login" : "setup",
    error: "",
  });
  renderAuthShell();
}

function renderMachineryManufacturerOptions() {
  const engineField = elements.engineForm?.elements?.namedItem("manufacturer");
  const generatorField = elements.generatorForm?.elements?.namedItem("manufacturer");
  if (engineField) {
    engineField.setAttribute("list", "engine-manufacturer-options");
    if (elements.engineManufacturerOptions) {
      elements.engineManufacturerOptions.innerHTML = vesselCatalogState.engineManufacturers
        .map((item) => `<option value="${escapeHtml(item.name)}"></option>`)
        .join("");
    }
  }

  if (generatorField) {
    generatorField.setAttribute("list", "generator-manufacturer-options");
    if (elements.generatorManufacturerOptions) {
      elements.generatorManufacturerOptions.innerHTML = vesselCatalogState.generatorManufacturers
        .map((item) => `<option value="${escapeHtml(item.name)}"></option>`)
        .join("");
    }
  }
}

function setVesselAddUiState(nextValues = {}) {
  Object.assign(vesselAddUiState, nextValues);
}

function renderVesselAddFeedback() {
  if (!elements.vesselAddFeedback) {
    return;
  }

  const message = String(vesselAddUiState.error || vesselAddUiState.message || "").trim();
  if (!message) {
    elements.vesselAddFeedback.hidden = true;
    elements.vesselAddFeedback.textContent = "";
    elements.vesselAddFeedback.classList.remove("system-note-error", "system-note-success");
    return;
  }

  elements.vesselAddFeedback.hidden = false;
  elements.vesselAddFeedback.textContent = message;
  elements.vesselAddFeedback.classList.toggle("system-note-error", Boolean(vesselAddUiState.error));
  elements.vesselAddFeedback.classList.toggle("system-note-success", !vesselAddUiState.error);
}

function renderVesselCatalogPreview() {
  if (!elements.vesselCatalogPreview) {
    return;
  }

  if (vesselCatalogState.loading && !vesselCatalogState.loaded) {
    elements.vesselCatalogPreview.innerHTML = `<div class="empty-state compact-empty-state">Loading the vessel catalog...</div>`;
    return;
  }

  if (vesselCatalogState.error) {
    elements.vesselCatalogPreview.innerHTML = `<div class="empty-state compact-empty-state">${escapeHtml(vesselCatalogState.error)}</div>`;
    return;
  }

  const selectedSpec = getSelectedCatalogSpec();
  if (!selectedSpec) {
    if (addVesselState.photoDataUrl) {
      elements.vesselCatalogPreview.innerHTML = `
        <div class="vessel-catalog-preview-shell">
          <div class="vessel-catalog-photo-shell">
            <img
              class="vessel-catalog-photo"
              src="${escapeHtml(addVesselState.photoDataUrl)}"
              alt="New vessel preview"
            />
          </div>
          <div class="empty-state compact-empty-state">Image staged. Select a year, brand, and model to preview specs before creating the vessel, or use Create custom vessel below.</div>
        </div>
      `;
      return;
    }

    elements.vesselCatalogPreview.innerHTML = `<div class="empty-state compact-empty-state">Select a year, brand, and model to preview the vessel specs before creating it.</div>`;
    return;
  }

  const previewName = addVesselState.customName.trim() || `${selectedSpec.manufacturerName} ${selectedSpec.modelName}`;
  const previewPhotoMarkup = addVesselState.photoDataUrl
    ? `
        <div class="vessel-catalog-photo-shell">
          <img
            class="vessel-catalog-photo"
            src="${escapeHtml(addVesselState.photoDataUrl)}"
            alt="${escapeHtml(previewName)} preview"
          />
        </div>
      `
    : "";
  const previewStats = [
    { label: "Type", value: selectedSpec.vesselType || "Pending" },
    { label: "Length", value: selectedSpec.length ? `${formatNumberValue(selectedSpec.length)} ft` : "Pending" },
    { label: "Beam", value: selectedSpec.beam ? `${formatNumberValue(selectedSpec.beam)} ft` : "Pending" },
    { label: "Draft", value: selectedSpec.draft ? `${formatNumberValue(selectedSpec.draft)} ft` : "Pending" },
    { label: "Fuel", value: selectedSpec.fuelCapacity ? `${formatNumberValue(selectedSpec.fuelCapacity)} gal` : "Pending" },
    { label: "Water", value: selectedSpec.waterCapacity ? `${formatNumberValue(selectedSpec.waterCapacity)} gal` : "Pending" },
    { label: "Black water", value: selectedSpec.blackWaterCapacity ? `${formatNumberValue(selectedSpec.blackWaterCapacity)} gal` : "Pending" },
    { label: "Hull", value: selectedSpec.hullMaterial || "Pending" },
  ];

  elements.vesselCatalogPreview.innerHTML = `
    <div class="vessel-catalog-preview-shell">
      <div class="card-topline">
        <div>
          <span class="metric-label">Catalog match</span>
          <h3 class="card-title">${escapeHtml(previewName)}</h3>
          <p class="card-meta">${escapeHtml(`${selectedSpec.year} | ${selectedSpec.manufacturerName} | ${selectedSpec.modelName}`)}</p>
        </div>
      </div>
      ${previewPhotoMarkup}
      <div class="system-meta-grid vessel-catalog-meta-grid">
        ${previewStats
          .map(
            (item) => `
              <div class="detail-item">
                <span class="detail-label">${escapeHtml(item.label)}</span>
                <span class="detail-value">${escapeHtml(item.value)}</span>
              </div>
            `
          )
          .join("")}
      </div>
      <div class="vessel-catalog-copy-grid">
        <div class="detail-item">
          <span class="detail-label">Engine info</span>
          <span class="detail-value">${escapeHtml(selectedSpec.engineInfo || "Pending")}</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Generator info</span>
          <span class="detail-value">${escapeHtml(selectedSpec.generatorInfo || "Pending")}</span>
        </div>
      </div>
    </div>
  `;
}

function populateVesselAddForm() {
  if (!elements.vesselAddForm) {
    return;
  }

  const fields = elements.vesselAddForm.elements;
  const yearField = fields.namedItem("catalogYear");
  const manufacturerField = fields.namedItem("catalogManufacturerId");
  const modelField = fields.namedItem("catalogModelId");
  const nameField = fields.namedItem("customName");
  const customManufacturerField = fields.namedItem("customManufacturer");
  const customModelField = fields.namedItem("customModel");

  nameField.value = addVesselState.customName;
  customManufacturerField.value = addVesselState.customManufacturer;
  customModelField.value = addVesselState.customModel;
  yearField.innerHTML = [
    `<option value="">Select year</option>`,
    ...vesselCatalogState.years.map((year) => `<option value="${year}" ${String(year) === String(addVesselState.year) ? "selected" : ""}>${year}</option>`),
  ].join("");

  const availableManufacturers = getAvailableCatalogManufacturers(addVesselState.year);
  if (addVesselState.manufacturerId && addVesselState.manufacturerId !== "__custom__" && !availableManufacturers.some((item) => item.id === addVesselState.manufacturerId)) {
    addVesselState.manufacturerId = "";
    addVesselState.modelId = "";
    addVesselState.specId = "";
  }

  manufacturerField.innerHTML = [
    `<option value="">Select brand</option>`,
    ...availableManufacturers.map((item) => `<option value="${escapeHtml(item.id)}" ${item.id === addVesselState.manufacturerId ? "selected" : ""}>${escapeHtml(item.name)}</option>`),
    `<option value="__custom__" ${addVesselState.manufacturerId === "__custom__" ? "selected" : ""}>Custom brand / manufacturer</option>`,
  ].join("");
  manufacturerField.disabled = vesselCatalogState.loading && !vesselCatalogState.loaded;

  const availableModels = getAvailableCatalogModels(addVesselState.year, addVesselState.manufacturerId);
  if (addVesselState.modelId && addVesselState.modelId !== "__custom__" && !availableModels.some((item) => item.id === addVesselState.modelId)) {
    addVesselState.modelId = "";
    addVesselState.specId = "";
  }

  modelField.innerHTML = [
    `<option value="">Select model</option>`,
    ...availableModels.map((item) => `<option value="${escapeHtml(item.id)}" ${item.id === addVesselState.modelId ? "selected" : ""}>${escapeHtml(item.name)}</option>`),
    `<option value="__custom__" ${addVesselState.modelId === "__custom__" ? "selected" : ""}>Custom model</option>`,
  ].join("");
  modelField.disabled = vesselCatalogState.loading && !vesselCatalogState.loaded;

  const selectedSpec = getSelectedCatalogSpec();
  addVesselState.specId = selectedSpec?.id || "";
  elements.clearVesselAddPhoto.hidden = !addVesselState.photoDataUrl;
  renderVesselCatalogPreview();
}

function createCatalogVesselSeed(spec) {
  return {
    name: addVesselState.customName.trim() || `${spec.manufacturerName} ${spec.modelName}`,
    builder: spec.manufacturerName,
    model: spec.modelName,
    yearBuilt: spec.year,
    vesselType: spec.vesselType,
    hullMaterial: spec.hullMaterial,
    length: spec.length,
    beam: spec.beam,
    draft: spec.draft,
    fuelCapacity: spec.fuelCapacity,
    waterCapacity: spec.waterCapacity,
    greyWaterCapacity: spec.greyWaterCapacity,
    blackWaterCapacity: spec.blackWaterCapacity,
    engineInfo: spec.engineInfo,
    generatorInfo: spec.generatorInfo,
    catalogManufacturerId: spec.manufacturerId,
    catalogModelId: spec.modelId,
    catalogSpecId: spec.id,
    isCustom: false,
    guests: defaultState.vessel.guests,
    status: "Docked",
    berth: "",
    captain: "",
    location: "",
    fuel: 0,
    waterTank: 0,
    greyTank: 0,
    blackTankLevel: 0,
    batteryStatus: 100,
    utilization: 0,
    photoDataUrl: addVesselState.photoDataUrl || "",
    notes: "",
    engines: [],
    generators: [],
    nextService: "",
  };
}

function handleVesselCatalogSelectionChange() {
  if (!elements.vesselAddForm) {
    return;
  }

  if (vesselAddUiState.error) {
    setVesselAddUiState({ error: "" });
  }

  const formData = new FormData(elements.vesselAddForm);
  addVesselState.customName = String(formData.get("customName") || "").trim();
  addVesselState.year = String(formData.get("catalogYear") || "").trim();
  addVesselState.manufacturerId = String(formData.get("catalogManufacturerId") || "").trim();
  addVesselState.modelId = String(formData.get("catalogModelId") || "").trim();
  addVesselState.customManufacturer = String(formData.get("customManufacturer") || "").trim();
  addVesselState.customModel = String(formData.get("customModel") || "").trim();

  if (addVesselState.manufacturerId !== "__custom__" && addVesselState.customManufacturer) {
    addVesselState.customManufacturer = "";
  }

  if (addVesselState.modelId !== "__custom__" && addVesselState.customModel) {
    addVesselState.customModel = "";
  }

  if (!addVesselState.manufacturerId) {
    addVesselState.modelId = "";
    addVesselState.specId = "";
  }

  if (addVesselState.manufacturerId === "__custom__") {
    addVesselState.modelId = "__custom__";
    addVesselState.specId = "";
  }

  if (!addVesselState.modelId || addVesselState.modelId === "__custom__") {
    addVesselState.specId = "";
  }

  populateVesselAddForm();
}

function handleCatalogVesselCreate(event) {
  event.preventDefault();
  handleVesselCatalogSelectionChange();

  if (addVesselState.manufacturerId === "__custom__" || addVesselState.modelId === "__custom__") {
    handleCreateCustomVessel();
    return;
  }

  const selectedSpec = getSelectedCatalogSpec();

  if (!selectedSpec) {
    setVesselAddUiState({
      error: "Select a year, brand, and model from the Harbor Command catalog, or switch to the custom option and type your own vessel details.",
      message: "",
    });
    renderVesselAddFeedback();
    return;
  }

  setVesselAddUiState({
    message: `Created ${addVesselState.customName.trim() || `${selectedSpec.manufacturerName} ${selectedSpec.modelName}`} and opened it in the profile editor.`,
    error: "",
  });
  addNewVesselProfile(createCatalogVesselSeed(selectedSpec));
}

function handleCreateCustomVessel() {
  handleVesselCatalogSelectionChange();
  const customBrand = addVesselState.customManufacturer.trim();
  const customModel = addVesselState.customModel.trim();
  if (!customBrand && !customModel && !addVesselState.customName.trim()) {
    setVesselAddUiState({
      error: "Add at least a vessel name, custom brand, or custom model before creating a custom vessel.",
      message: "",
    });
    renderVesselAddFeedback();
    return;
  }

  setVesselAddUiState({
    message: `Created ${addVesselState.customName.trim() || customModel || `Vessel ${state.vessels.length + 1}`} and opened it in the profile editor.`,
    error: "",
  });
  addNewVesselProfile({
    name: addVesselState.customName.trim() || customModel || `Vessel ${state.vessels.length + 1}`,
    builder: customBrand,
    model: customModel,
    yearBuilt: normalizePositiveInteger(addVesselState.year, 0, 0),
    photoDataUrl: addVesselState.photoDataUrl || "",
    isCustom: true,
  });
}

function handleVesselAddPhotoChange(event) {
  const [file] = event.currentTarget.files || [];
  if (!file) {
    return;
  }

  const reader = new FileReader();
  reader.addEventListener("load", () => {
    addVesselState.photoDataUrl = typeof reader.result === "string" ? reader.result : "";
    event.currentTarget.value = "";
    populateVesselAddForm();
  });
  reader.readAsDataURL(file);
}

function clearVesselAddPhoto() {
  addVesselState.photoDataUrl = "";
  if (elements.vesselAddPhotoInput) {
    elements.vesselAddPhotoInput.value = "";
  }
  populateVesselAddForm();
}

function handleVesselSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);

  state.vessel = {
    ...state.vessel,
    name: String(formData.get("name")).trim(),
    builder: String(formData.get("builder")).trim(),
    model: String(formData.get("model")).trim(),
    yearBuilt: normalizePositiveInteger(formData.get("yearBuilt"), state.vessel.yearBuilt, 0),
    vesselType: String(formData.get("vesselType")).trim(),
    hullMaterial: String(formData.get("hullMaterial")).trim(),
    captain: String(formData.get("captain")).trim(),
    location: String(formData.get("location")).trim(),
    length: Number(formData.get("length")),
    beam: Number(formData.get("beam")),
    draft: Number(formData.get("draft")),
    guests: Number(formData.get("guests")),
    status: String(formData.get("status")),
    berth: String(formData.get("berth")).trim(),
    notes: String(formData.get("notes")).trim(),
  };

  state.vessel.name = state.vessel.name || `Vessel ${state.vessels.findIndex((vessel) => vessel.id === state.activeVesselId) + 1}`;
  persistAndRender();
}

function handleVesselSystemsSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);

  state.vessel = {
    ...state.vessel,
    fuel: clampPercent(formData.get("fuel")),
    waterTank: clampPercent(formData.get("waterTank")),
    greyTank: clampPercent(formData.get("greyTank")),
    blackTankLevel: clampPercent(formData.get("blackTankLevel")),
    batteryStatus: clampPercent(formData.get("batteryStatus")),
    utilization: clampPercent(formData.get("utilization")),
    fuelCapacity: normalizePositiveInteger(formData.get("fuelCapacity"), state.vessel.fuelCapacity, 0),
    waterCapacity: normalizePositiveInteger(formData.get("waterCapacity"), state.vessel.waterCapacity, 0),
    greyWaterCapacity: normalizePositiveInteger(formData.get("greyWaterCapacity"), state.vessel.greyWaterCapacity, 0),
    blackWaterCapacity: normalizePositiveInteger(formData.get("blackWaterCapacity"), state.vessel.blackWaterCapacity, 0),
    nextService: String(formData.get("nextService") || "").trim(),
    engineInfo: String(formData.get("engineInfo") || "").trim(),
    generatorInfo: String(formData.get("generatorInfo") || "").trim(),
  };
  state.vessel.engineInfo = buildMachinerySummary(state.vessel.engines, state.vessel.engineInfo);
  state.vessel.generatorInfo = buildMachinerySummary(state.vessel.generators, state.vessel.generatorInfo);

  state.activeView = "vessel";
  persistAndRender();
}

function handleEngineSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const engineValues = normalizeMachineryItem(
    {
      id: editingEngineId || createId("engine"),
      label: String(formData.get("label")).trim(),
      manufacturer: String(formData.get("manufacturer")).trim(),
      model: String(formData.get("model")).trim(),
      rating: String(formData.get("rating")).trim(),
      hours: formData.get("hours"),
      lastServiceHours: formData.get("lastServiceHours"),
      serviceIntervalHours: formData.get("serviceIntervalHours"),
      lastServiceDate: String(formData.get("lastServiceDate") || ""),
      nextServiceDate: String(formData.get("nextServiceDate") || ""),
      notes: String(formData.get("notes")).trim(),
    },
    {},
    "engine"
  );

  if (editingEngineId) {
    const engine = state.vessel.engines.find((item) => item.id === editingEngineId);
    if (engine) {
      Object.assign(engine, engineValues);
    }
  } else {
    state.vessel.engines.unshift(engineValues);
  }

  state.vessel.engineInfo = buildMachinerySummary(state.vessel.engines, "");
  resetEngineForm();
  persistAndRender();
}

function handleGeneratorSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const generatorValues = normalizeMachineryItem(
    {
      id: editingGeneratorId || createId("generator"),
      label: String(formData.get("label")).trim(),
      manufacturer: String(formData.get("manufacturer")).trim(),
      model: String(formData.get("model")).trim(),
      rating: String(formData.get("rating")).trim(),
      hours: formData.get("hours"),
      lastServiceHours: formData.get("lastServiceHours"),
      serviceIntervalHours: formData.get("serviceIntervalHours"),
      lastServiceDate: String(formData.get("lastServiceDate") || ""),
      nextServiceDate: String(formData.get("nextServiceDate") || ""),
      notes: String(formData.get("notes")).trim(),
    },
    {},
    "generator"
  );

  if (editingGeneratorId) {
    const generator = state.vessel.generators.find((item) => item.id === editingGeneratorId);
    if (generator) {
      Object.assign(generator, generatorValues);
    }
  } else {
    state.vessel.generators.unshift(generatorValues);
  }

  state.vessel.generatorInfo = buildMachinerySummary(state.vessel.generators, "");
  resetGeneratorForm();
  persistAndRender();
}

function addNewVesselProfile(overrides = {}) {
  const vesselId = createId("vessel");
  const vesselNumber = state.vessels.length + 1;
  const newVessel = normalizeVesselRecord(
    {
      id: vesselId,
      ...defaultState.vessel,
      name: overrides.name || `Vessel ${vesselNumber}`,
      builder: "",
      model: "",
      yearBuilt: 0,
      vesselType: "",
      hullMaterial: "",
      captain: "",
      location: "",
      berth: "",
      notes: "",
      photoDataUrl: "",
      engineInfo: "",
      generatorInfo: "",
      engines: [],
      generators: [],
      status: "Docked",
      fuel: 0,
      waterTank: 0,
      greyTank: 0,
      blackTankLevel: 0,
      batteryStatus: 100,
      utilization: 0,
      catalogManufacturerId: "",
      catalogModelId: "",
      catalogSpecId: "",
      isCustom: true,
      nextService: "",
      ...overrides,
    },
    defaultState.vessel,
    vesselNumber - 1
  );

  state.vessels.push(newVessel);
  state.vesselBundles[newVessel.id] = createFreshVesselBundle();
  activateRuntimeVesselState(state, newVessel.id);
  Object.assign(addVesselState, {
    customName: "",
    year: "",
    manufacturerQuery: "",
    manufacturerId: "",
    modelQuery: "",
    modelId: "",
    specId: "",
    customManufacturer: "",
    customModel: "",
    photoDataUrl: "",
  });
  state.activeView = "vessel";
  persistAndRender();
  window.requestAnimationFrame(() => {
    document.querySelector("#vessel-form-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

function handleDeleteActiveVessel() {
  if (state.vessels.length <= 1) {
    window.alert("Harbor Command needs at least one vessel profile. Add another vessel before deleting this one.");
    return;
  }

  const activeVessel = state.vessels.find((vessel) => String(vessel.id) === String(state.activeVesselId));
  if (!activeVessel) {
    return;
  }

  const confirmed = window.confirm(`Delete ${activeVessel.name} and its linked Harbor Command data? This removes its maintenance, work orders, reports, inventory, expenses, crew, vendors, and voyage records.`);
  if (!confirmed) {
    return;
  }

  state.vessels = state.vessels.filter((vessel) => String(vessel.id) !== String(activeVessel.id));
  delete state.vesselBundles[activeVessel.id];

  const fallbackVessel = state.vessels[0];
  if (!fallbackVessel) {
    return;
  }

  activateRuntimeVesselState(state, fallbackVessel.id);
  editingEngineId = null;
  editingGeneratorId = null;
  editingMaintenanceId = null;
  editingWorkOrderId = null;
  editingVendorId = null;
  editingInventoryId = null;
  editingExpenseId = null;
  persistAndRender();
}

function handleVesselPhotoChange(event) {
  const [file] = event.currentTarget.files || [];

  if (!file) {
    return;
  }

  const reader = new FileReader();
  reader.addEventListener("load", () => {
    state.vessel = {
      ...state.vessel,
      photoDataUrl: typeof reader.result === "string" ? reader.result : "",
    };
    event.currentTarget.value = "";
    persistAndRender();
  });
  reader.readAsDataURL(file);
}

function clearVesselPhoto() {
  state.vessel = {
    ...state.vessel,
    photoDataUrl: "",
  };
  if (elements.vesselPhotoInput) {
    elements.vesselPhotoInput.value = "";
  }
  persistAndRender();
}

function handleMaintenanceAssetFormChange(event) {
  const form = elements.maintenanceAssetForm;
  if (!form) {
    return;
  }

  const templateField = form.elements.namedItem("templateId");
  const assetTypeField = form.elements.namedItem("assetType");
  const nameField = form.elements.namedItem("name");
  const meterSourceTypeField = form.elements.namedItem("meterSourceType");
  const meterSourceIdField = form.elements.namedItem("meterSourceId");
  const selectedTemplate = getMaintenanceTemplateById(templateField?.value || "");

  if (event?.target?.name === "templateId" && selectedTemplate) {
    if (!assetTypeField.value.trim()) {
      assetTypeField.value = selectedTemplate.assetType || "";
    }
    if (!nameField.value.trim()) {
      nameField.value = selectedTemplate.name || "";
    }
    if (!meterSourceTypeField.value || meterSourceTypeField.value === "none") {
      if (selectedTemplate.assetType === "main-engine" && state.vessel.engines.length) {
        meterSourceTypeField.value = "engine";
      } else if (selectedTemplate.assetType === "generator" && state.vessel.generators.length) {
        meterSourceTypeField.value = "generator";
      }
    }
  }

  meterSourceIdField.innerHTML = buildMaintenanceMeterSourceOptions(
    meterSourceTypeField.value,
    meterSourceIdField.value
  );
  renderMaintenanceTemplatePreview(selectedTemplate);
}

function handleMaintenanceAssetSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const existingAsset = state.maintenanceAssets.find((item) => item.id === editingMaintenanceAssetId) || null;
  const asset = buildMaintenanceAssetFromForm(formData, existingAsset);
  const selectedTemplate = getMaintenanceTemplateById(asset.templateId);

  if (editingMaintenanceAssetId) {
    const assetIndex = state.maintenanceAssets.findIndex((item) => String(item.id) === String(editingMaintenanceAssetId));
    if (assetIndex >= 0) {
      state.maintenanceAssets[assetIndex] = asset;
      state.maintenance = state.maintenance.map((task) =>
        String(task.assetId) === String(asset.id)
          ? normalizeMaintenanceItem({
              ...task,
              assetId: asset.id,
              templateId: asset.templateId || task.templateId || "",
              meterSourceType: asset.meterSourceType || task.meterSourceType,
              meterSourceId: asset.meterSourceId || "",
              updatedAt: currentIsoStamp(),
            })
          : task
      );
    }
  } else {
    state.maintenanceAssets.unshift(asset);
    if (selectedTemplate) {
      state.maintenance.unshift(...buildMaintenanceTasksFromTemplate(asset, selectedTemplate));
    }
  }

  resetMaintenanceAssetForm();
  state.activeView = "maintenance";
  state.activeMaintenanceWorkspace = "systems";
  persistAndRender();
}

function handleMaintenanceSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const existingTask = state.maintenance.find((item) => item.id === editingMaintenanceId) || null;
  const assetId = String(formData.get("assetId") || "").trim();
  const linkedAsset = getMaintenanceAssetById(assetId);
  const recurrenceMode = normalizeMaintenanceRecurrenceMode(
    formData.get("recurrenceMode"),
    normalizePositiveInteger(formData.get("intervalDays"), DEFAULT_MAINTENANCE_INTERVAL_DAYS, 0),
    normalizePositiveInteger(formData.get("intervalHours"), DEFAULT_MAINTENANCE_INTERVAL_HOURS, 0)
  );

  const maintenanceValues = normalizeMaintenanceItem({
    id: editingMaintenanceId || createId("maintenance-task"),
    title: String(formData.get("title") || "").trim(),
    category: String(formData.get("category") || "").trim() || linkedAsset?.assetType || existingTask?.category || "General",
    assetId,
    templateId: existingTask?.templateId || linkedAsset?.templateId || "",
    templateTaskId: existingTask?.templateTaskId || "",
    dueDate: String(formData.get("dueDate") || "").trim(),
    dueHours: normalizePositiveInteger(formData.get("dueHours"), existingTask?.dueHours || 0, 0),
    status: String(formData.get("status") || "Not Started"),
    priority: String(formData.get("priority") || "High"),
    lastCompleted: String(formData.get("lastCompleted") || "").trim(),
    lastCompletedHours: normalizePositiveInteger(formData.get("lastCompletedHours"), existingTask?.lastCompletedHours || 0, 0),
    intervalDays: normalizePositiveInteger(formData.get("intervalDays"), DEFAULT_MAINTENANCE_INTERVAL_DAYS, 0),
    intervalHours: normalizePositiveInteger(formData.get("intervalHours"), DEFAULT_MAINTENANCE_INTERVAL_HOURS, 0),
    reminderDays: normalizePositiveInteger(formData.get("reminderDays"), DEFAULT_MAINTENANCE_REMINDER_DAYS, 0),
    reminderHours: normalizePositiveInteger(formData.get("reminderHours"), DEFAULT_MAINTENANCE_REMINDER_HOURS, 0),
    recurrenceMode,
    meterSourceType: String(formData.get("meterSourceType") || linkedAsset?.meterSourceType || "none").trim().toLowerCase() || "none",
    meterSourceId: String(formData.get("meterSourceId") || linkedAsset?.meterSourceId || "").trim(),
    isCustom: existingTask?.isCustom === false ? false : true,
    notes: String(formData.get("notes") || "").trim(),
    createdAt: existingTask?.createdAt || currentIsoStamp(),
    updatedAt: currentIsoStamp(),
    sortOrder: existingTask?.sortOrder ?? state.maintenance.length,
  });

  if (maintenanceValues.status === "Completed") {
    if (usesDateRecurrence(maintenanceValues) && !maintenanceValues.lastCompleted) {
      maintenanceValues.lastCompleted = todayStamp();
    }
    if (usesHourRecurrence(maintenanceValues) && !maintenanceValues.lastCompletedHours) {
      maintenanceValues.lastCompletedHours = getMaintenanceCurrentHours(maintenanceValues);
    }
    if (usesDateRecurrence(maintenanceValues) && maintenanceValues.lastCompleted && getMaintenanceIntervalDays(maintenanceValues) > 0) {
      maintenanceValues.dueDate = addDaysToDate(maintenanceValues.lastCompleted, getMaintenanceIntervalDays(maintenanceValues));
    }
    if (usesHourRecurrence(maintenanceValues) && getMaintenanceIntervalHours(maintenanceValues) > 0) {
      maintenanceValues.dueHours = maintenanceValues.lastCompletedHours + getMaintenanceIntervalHours(maintenanceValues);
    }
  }

  if (editingMaintenanceId) {
    const taskIndex = state.maintenance.findIndex((item) => item.id === editingMaintenanceId);
    if (taskIndex >= 0) {
      state.maintenance[taskIndex] = maintenanceValues;
    }
  } else {
    state.maintenance.unshift(maintenanceValues);
  }

  state.activeView = "maintenance";
  state.activeMaintenanceWorkspace = "new-service-task";
  resetMaintenanceForm();
  persistAndRender();
}

function handleMaintenanceTaskFormChange(event) {
  const form = elements.maintenanceForm;
  if (!form) {
    return;
  }

  const assetField = form.elements.namedItem("assetId");
  const meterSourceTypeField = form.elements.namedItem("meterSourceType");
  const meterSourceIdField = form.elements.namedItem("meterSourceId");
  const categoryField = form.elements.namedItem("category");
  const recurrenceField = form.elements.namedItem("recurrenceMode");
  const linkedAsset = getMaintenanceAssetById(assetField.value);

  if (event?.target?.name === "assetId" && linkedAsset) {
    if (!categoryField.value.trim()) {
      const template = getMaintenanceTemplateById(linkedAsset.templateId);
      categoryField.value = template?.tasks?.[0]?.category || linkedAsset.assetType || "";
    }
    if (!meterSourceTypeField.value || meterSourceTypeField.value === "none") {
      meterSourceTypeField.value = linkedAsset.meterSourceType || "none";
    }
    if (!meterSourceIdField.value) {
      meterSourceIdField.value = linkedAsset.meterSourceId || "";
    }
    if (!recurrenceField.value) {
      recurrenceField.value = linkedAsset.meterSourceType === "engine" || linkedAsset.meterSourceType === "generator"
        ? "days-or-hours"
        : "days";
    }
  }

  meterSourceIdField.innerHTML = buildMaintenanceMeterSourceOptions(
    meterSourceTypeField.value,
    linkedAsset && (meterSourceTypeField.value === linkedAsset.meterSourceType) ? linkedAsset.meterSourceId : meterSourceIdField.value
  );
}

function handleWorkOrderSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const currentWeek = getActiveWorkWeekRange();
  const reportDate = String(formData.get("reportDate") || "").trim() || getPreferredCurrentWorkOrderDate();

  if (!isDateWithinRange(reportDate, currentWeek.start, currentWeek.end)) {
    window.alert(`Weekly workspace dates must stay between ${formatDate(currentWeek.start)} and ${formatDate(currentWeek.end)}.`);
    return;
  }

  const workOrderValues = normalizeWorkOrderItem(
    {
      id: editingWorkOrderId || createId("work-order"),
      item: String(formData.get("item")).trim(),
      reportDate,
      maintenanceLogId: String(formData.get("maintenanceLogId") || "").trim(),
      workDone: String(formData.get("workDone")).trim(),
      systemsChecked: String(formData.get("systemsChecked")).trim(),
      issues: String(formData.get("issues")).trim(),
      notes: String(formData.get("notes")).trim(),
      weekStart: currentWeek.start,
      weekEnd: currentWeek.end,
      status: editingWorkOrderId
        ? state.workOrders.find((item) => item.id === editingWorkOrderId)?.status || "Open"
        : "Open",
      originType: editingWorkOrderId
        ? state.workOrders.find((item) => item.id === editingWorkOrderId)?.originType || "manual"
        : "manual",
      completedAt: editingWorkOrderId
        ? state.workOrders.find((item) => item.id === editingWorkOrderId)?.completedAt || ""
        : "",
      updatedAt: currentIsoStamp(),
      createdAt: editingWorkOrderId
        ? state.workOrders.find((item) => item.id === editingWorkOrderId)?.createdAt || currentIsoStamp()
        : currentIsoStamp(),
    }
  );

  if (editingWorkOrderId) {
    const order = state.workOrders.find((item) => item.id === editingWorkOrderId);
    if (order) {
      Object.assign(order, workOrderValues);
    }
  } else {
    state.workOrders.unshift({
      id: createId("work-order"),
      ...workOrderValues,
    });
  }

  state.activeView = "work-orders";
  resetWorkOrderForm();
  persistAndRender();
}

function handleCharterSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);

  state.charters.unshift({
    id: createId("charter"),
    client: String(formData.get("client")).trim(),
    start: String(formData.get("start")),
    end: String(formData.get("end")),
    berth: String(formData.get("berth")).trim(),
    status: "Confirmed",
  });

  state.vessel.status = "Charter";

  event.currentTarget.reset();
  seedFormDates();
  persistAndRender();
}

function handleReportHistoryChange(event) {
  state.activeReportId = String(event.currentTarget.value || "");
  cacheLocalState();
  renderApp();
}

async function handleExportWeeklyReportPdf(event) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }

  const selectedReport = getSelectedWeeklyReport();
  if (!selectedReport) {
    window.alert("Select or start a weekly report before exporting it.");
    return;
  }

  const pdfUrl = `/api/weekly-reports/${encodeURIComponent(selectedReport.id)}/pdf`;
  const popup = window.open(pdfUrl, "_blank", "noopener,noreferrer");
  if (!popup) {
    const link = document.createElement("a");
    link.href = pdfUrl;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    document.body.appendChild(link);
    link.click();
    link.remove();
  }
}

async function handleGenerateWeeklyReportFromWorkspace() {
  const currentWeekEntries = getCurrentWeekWorkOrders();
  const currentWeek = getActiveWorkWeekRange();

  if (!currentWeekEntries.length) {
    window.alert(`Add at least one entry in Work Orders for ${formatDate(currentWeek.start)} through ${formatDate(currentWeek.end)} before generating a report.`);
    return;
  }

  try {
    await saveStateSnapshot(snapshotStateForPersistence());
    const generatedReport = await generateWeeklyReportRequest(currentWeek.start);
    upsertWeeklyReportState(generatedReport);
    state.activeReportId = generatedReport.id;
    state.activeView = "reports";
    cacheLocalState();
    renderApp();
  } catch (error) {
    window.alert(error instanceof Error ? error.message : "Unable to generate the weekly report.");
  }
}

async function handleToggleWeeklyReportStatus() {
  const selectedReport = getSelectedWeeklyReport();
  if (!selectedReport) {
    window.alert("Select a generated report from the report library first.");
    return;
  }

  try {
    const nextStatus = selectedReport.status === "finalized" ? "draft" : "finalized";
    const activeWorkWeek = getActiveWorkWeekRange();
    const shouldAdvanceWorkspace =
      nextStatus === "finalized"
      && selectedReport.weekStart === activeWorkWeek.start
      && selectedReport.weekEnd === activeWorkWeek.end;
    const updatedReport = await updateWeeklyReportRequest(selectedReport.id, nextStatus);
    upsertWeeklyReportState(updatedReport);
    if (shouldAdvanceWorkspace) {
      state.activeWorkWeekStart = addDaysToDate(selectedReport.weekStart, 7);
      persistAndRender();
      return;
    }
    cacheLocalState();
    renderApp();
  } catch (error) {
    window.alert(error instanceof Error ? error.message : "Unable to update the weekly report status.");
  }
}

function handleVendorSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const vendorValues = {
    name: String(formData.get("name")).trim(),
    contact: String(formData.get("contact")).trim(),
    email: String(formData.get("email")).trim() || "N/A",
    phone: String(formData.get("phone")).trim(),
    status: String(formData.get("status")),
    category: String(formData.get("category")).trim(),
  };

  if (editingVendorId) {
    const vendor = state.vendors.find((item) => item.id === editingVendorId);
    if (vendor) {
      Object.assign(vendor, vendorValues);
    }
  } else {
    state.vendors.unshift({
      id: createId("vendor"),
      ...vendorValues,
    });
  }

  state.activeView = "vendors";
  state.activeVendorFilter = "all";
  resetVendorForm();
  persistAndRender();
}

function handleInventorySubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const inventoryValues = {
    name: String(formData.get("name")).trim(),
    location: String(formData.get("location")).trim(),
    quantity: Number(formData.get("quantity")),
    unit: String(formData.get("unit")).trim(),
    minimumQuantity: Number(formData.get("minimumQuantity")),
    status: String(formData.get("status")),
    notes: String(formData.get("notes")).trim(),
  };

  if (editingInventoryId) {
    const item = state.inventory.find((entry) => entry.id === editingInventoryId);
    if (item) {
      Object.assign(item, inventoryValues);
    }
  } else {
    state.inventory.unshift({
      id: createId("inventory"),
      ...inventoryValues,
    });
  }

  state.activeView = "inventory";
  resetInventoryForm();
  persistAndRender();
}

function handleExpenseSubmit(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  const expenseValues = {
    title: String(formData.get("title")).trim(),
    vendor: String(formData.get("vendor")).trim(),
    category: String(formData.get("category")).trim(),
    amount: Number(formData.get("amount")),
    currency: String(formData.get("currency")).trim().toUpperCase() || "USD",
    expenseDate: String(formData.get("expenseDate")),
    status: String(formData.get("status")),
    notes: String(formData.get("notes")).trim(),
  };

  if (editingExpenseId) {
    const expense = state.expenses.find((entry) => entry.id === editingExpenseId);
    if (expense) {
      Object.assign(expense, expenseValues);
    }
  } else {
    state.expenses.unshift({
      id: createId("expense"),
      ...expenseValues,
    });
  }

  state.activeView = "expenses";
  resetExpenseForm();
  persistAndRender();
}

function handleExpenseVendorChange(event) {
  const vendor = findVendorByName(event.currentTarget.value);
  if (!vendor) {
    if (!event.currentTarget.value) {
      elements.expenseForm.elements.namedItem("category").value = "";
    }
    return;
  }

  elements.expenseForm.elements.namedItem("category").value = vendor.category || "";
}

async function handleInviteSubmit(event) {
  event.preventDefault();

  if (!canManageUsers()) {
    return;
  }

  const formData = new FormData(event.currentTarget);
  const role = String(formData.get("role"));
  const payload = {
    email: String(formData.get("email")).trim(),
    role,
    vesselIds: roleHasFullFleetAccess(role) ? [] : readCheckedVesselIds(elements.inviteVesselPicker),
  };

  if (!roleHasFullFleetAccess(role) && !payload.vesselIds.length && getManageableVesselsForAccessModal().length) {
    setManagedInvitesState({
      loading: false,
      error: "Select at least one vessel for this invite.",
      notice: "",
      latestInvite: null,
    });
    renderAccessModal();
    return;
  }

  setManagedInvitesState({
    loading: true,
    error: "",
    notice: "",
    latestInvite: null,
  });
  renderAccessModal();

  try {
    const response = await apiFetch("/invites", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();

    if (!response.ok) {
      throw new Error(result?.error || "Unable to create invite.");
    }

    resetInviteForm();
    const inviteDelivery = normalizeInviteDelivery(result?.delivery);
    setManagedInvitesState({
      notice: result?.emailSent
        ? `Invite created and emailed to ${payload.email}.`
        : result?.emailError
          ? `Invite created for ${payload.email}. Email not sent: ${result.emailError}`
          : `Invite created for ${payload.email}.`,
      error: "",
      latestInvite: result?.invite || null,
      delivery: inviteDelivery,
    });
    await loadManagedInvites();
  } catch (error) {
    setManagedInvitesState({
      loading: false,
      error: error instanceof Error ? error.message : "Unable to create invite.",
      notice: "",
      latestInvite: null,
    });
    renderAccessModal();
  }
}

async function handleManagedUserAction(actionButton) {
  if (!canManageUsers()) {
    return;
  }

  const userId = actionButton.dataset.userId;
  const action = actionButton.dataset.userAction;
  const userName = actionButton.dataset.userName || "this account";
  const targetUser = managedUsersState.items.find((user) => String(user.id) === String(userId));

  if (!userId || !action) {
    return;
  }

  setManagedUsersState({
    loading: true,
    error: "",
    notice: "",
  });
  renderAccessModal();

  try {
    if (action === "access") {
      if (!targetUser || roleHasFullFleetAccess(targetUser.role)) {
        setManagedUsersState({
          loading: false,
          notice: `${userName} already has full fleet access through role.`,
          error: "",
        });
        renderAccessModal();
        return;
      }

      const card = actionButton.closest(".account-card");
      const vesselIds = readCheckedVesselIds(card);
      if (!vesselIds.length && getManageableVesselsForAccessModal().length) {
        throw new Error("Select at least one vessel for this user.");
      }

      const response = await apiFetch(`/users/${userId}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ vesselIds }),
      });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result?.error || "Unable to update vessel access.");
      }

      setManagedUsersState({
        notice: `Updated vessel access for ${result.user?.name || userName}.`,
        error: "",
      });
      await loadManagedUsers();
      return;
    }

    if (action === "toggle") {
      const nextActiveState = actionButton.dataset.userActive !== "true";
      const response = await apiFetch(`/users/${userId}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ isActive: nextActiveState }),
      });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result?.error || "Unable to update user.");
      }

      setManagedUsersState({
        notice: `${result.user?.name || userName} is now ${result.user?.isActive ? "active" : "inactive"}.`,
        error: "",
      });
      await loadManagedUsers();
      return;
    }

    if (action === "delete") {
      const shouldDelete = window.confirm(`Delete ${userName} from Harbor Command?`);
      if (!shouldDelete) {
        setManagedUsersState({
          loading: false,
        });
        renderAccessModal();
        return;
      }

      const response = await apiFetch(`/users/${userId}`, {
        method: "DELETE",
      });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result?.error || "Unable to delete user.");
      }

      setManagedUsersState({
        notice: `${userName} was removed from Harbor Command.`,
        error: "",
      });
      await loadManagedUsers();
      return;
    }
  } catch (error) {
    setManagedUsersState({
      loading: false,
      error: error instanceof Error ? error.message : "Unable to update user.",
      notice: "",
    });
    renderAccessModal();
  }
}

async function handleInviteAction(actionButton) {
  if (!canManageUsers()) {
    return;
  }

  const action = actionButton.dataset.inviteAction;
  const inviteId = actionButton.dataset.inviteId;
  const inviteToken = actionButton.dataset.inviteToken || "";
  const inviteEmail = actionButton.dataset.inviteEmail || "";
  const inviteRole = actionButton.dataset.inviteRole || "Crew";
  const inviteLink = inviteToken ? buildInviteLink(inviteToken) : "";

  if (action === "copy" && inviteLink) {
    try {
      await navigator.clipboard.writeText(inviteLink);
      setManagedInvitesState({
        notice: `Invite link copied for ${inviteEmail}.`,
        error: "",
      });
    } catch (error) {
      setManagedInvitesState({
        error: "Unable to copy the invite link in this browser.",
        notice: "",
      });
    }
    renderAccessModal();
    return;
  }

  if (action === "email" && inviteId) {
    setManagedInvitesState({
      loading: true,
      error: "",
      notice: "",
    });
    renderAccessModal();

    try {
      const response = await apiFetch(`/invites/${inviteId}/send`, {
        method: "POST",
      });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result?.error || "Unable to send invite email.");
      }

      setManagedInvitesState({
        loading: false,
        error: "",
        notice: `Invite email sent to ${inviteEmail}.`,
        delivery: normalizeInviteDelivery(result?.delivery),
      });
      renderAccessModal();
      return;
    } catch (error) {
      setManagedInvitesState({
        loading: false,
        error: error instanceof Error ? error.message : "Unable to send invite email.",
        notice: "",
      });
      renderAccessModal();
      return;
    }
  }

  if (action === "email" && inviteLink) {
    window.location.href = buildInviteMailto(inviteEmail, inviteLink, inviteRole);
    return;
  }

  if (action === "revoke" && inviteId) {
    const shouldRevoke = window.confirm(`Revoke the invite for ${inviteEmail}?`);
    if (!shouldRevoke) {
      return;
    }

    setManagedInvitesState({
      loading: true,
      error: "",
      notice: "",
    });
    renderAccessModal();

    try {
      const response = await apiFetch(`/invites/${inviteId}`, {
        method: "DELETE",
      });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result?.error || "Unable to revoke invite.");
      }

      setManagedInvitesState({
        notice: `Invite revoked for ${inviteEmail}.`,
        error: "",
        latestInvite: managedInvitesState.latestInvite?.id === Number(inviteId) ? null : managedInvitesState.latestInvite,
      });
      await loadManagedInvites();
      return;
    } catch (error) {
      setManagedInvitesState({
        loading: false,
        error: error instanceof Error ? error.message : "Unable to revoke invite.",
        notice: "",
      });
      renderAccessModal();
    }
  }
}

function renderApp() {
  ensureAccessibleView();
  ensureActiveReportSelection();
  seedFormDates();
  renderMaintenanceAssetForm();
  renderMaintenanceForm();
  renderWorkOrderForm();
  renderVendorForm();
  renderInventoryForm();
  renderExpenseForm();
  renderSummary();
  populateVesselAddForm();
  renderVesselAddFeedback();
  renderMachineryManufacturerOptions();
  populateVesselForm();
  renderVesselSystems();
  populateVesselSystemsForm();
  renderEngineForm();
  renderGeneratorForm();
  renderEngineList();
  renderGeneratorList();
  renderSpotlight();
  renderOverview();
  renderMaintenanceAssets();
  renderMaintenanceHistory();
  renderMaintenance();
  renderWorkOrders();
  renderCharters();
  renderCrew();
  renderAccessModal();
  renderReports();
  renderVendors();
  renderInventory();
  renderExpenses();
  renderVoyages();
  renderAlerts();
  renderViewPanels();
  ensureWeatherData();
}

function renderSummary() {
  const openTasks = state.maintenance.filter((task) => !isMaintenanceComplete(task)).length;
  const currentWeekEntries = getCurrentWeekWorkOrders().length;
  const reportCount = state.reports.length;

  const cards = [
    {
      label: "Vessel status",
      value: state.vessel.status,
      subtext: `${state.vessel.name} at ${state.vessel.berth}`,
      light: false,
    },
    {
      label: "Open maintenance",
      value: `${openTasks}`,
      subtext: "Maintenance tasks awaiting completion",
      light: false,
    },
    {
      label: "Weekly workspace",
      value: `${currentWeekEntries}`,
      subtext: "Saved entries in this Monday through Friday workspace",
      light: true,
    },
    {
      label: "Saved reports",
      value: `${reportCount}`,
      subtext: "Generated weekly reports in the vessel library",
      light: true,
    },
  ];

  elements.summaryCards.innerHTML = cards
    .map(
      (card) => `
        <article class="metric-card ${card.light ? "light" : ""}">
          <span class="metric-label">${card.label}</span>
          <span class="metric-value metric-value-tight">${card.value}</span>
          <p class="metric-subtext">${card.subtext}</p>
        </article>
      `
    )
    .join("");
}

function populateVesselForm() {
  const fields = elements.vesselForm.elements;

  fields.namedItem("name").value = state.vessel.name;
  fields.namedItem("builder").value = state.vessel.builder || "";
  fields.namedItem("model").value = state.vessel.model;
  fields.namedItem("yearBuilt").value = state.vessel.yearBuilt || "";
  fields.namedItem("vesselType").value = state.vessel.vesselType || "";
  fields.namedItem("captain").value = state.vessel.captain;
  fields.namedItem("location").value = state.vessel.location;
  fields.namedItem("hullMaterial").value = state.vessel.hullMaterial || "";
  fields.namedItem("length").value = state.vessel.length;
  fields.namedItem("beam").value = state.vessel.beam;
  fields.namedItem("draft").value = state.vessel.draft;
  fields.namedItem("guests").value = state.vessel.guests;
  fields.namedItem("status").value = state.vessel.status;
  fields.namedItem("berth").value = state.vessel.berth;
  fields.namedItem("notes").value = state.vessel.notes;
  elements.clearVesselPhoto.hidden = !state.vessel.photoDataUrl;
}

function renderVesselSystems() {
  const openTasks = state.maintenance.filter((task) => !isMaintenanceComplete(task)).length;
  const currentWeekEntries = getCurrentWeekWorkOrders().length;
  const activeVendors = state.vendors.filter((vendor) => String(vendor.status || "").toLowerCase() === "active").length;
  const reportRows = state.reports.length;
  const selectorSummary = [
    { label: "Fleet", value: `${state.vessels.length} vessel${state.vessels.length === 1 ? "" : "s"}` },
    { label: "Open maintenance", value: `${openTasks}` },
    { label: "This week", value: `${currentWeekEntries} entries` },
  ];
  const selectorMarkup = `
    <div class="vessel-selector-shell">
      <div class="vessel-selector-topline">
        <div>
          <p class="eyebrow">Fleet Control</p>
          <h3>${state.vessels.length > 1 ? "Switch active vessel" : "Your vessel profile"}</h3>
          <p class="small-copy">Keep the active yacht in focus while all other vessel sections follow the same profile context.</p>
        </div>
        <div class="vessel-selector-summary">
          ${selectorSummary
            .map(
              (item) => `
                <article class="vessel-selector-stat">
                  <span class="detail-label">${item.label}</span>
                  <strong>${escapeHtml(item.value)}</strong>
                </article>
              `
            )
            .join("")}
        </div>
      </div>
      <div class="vessel-selector-rail">
        ${state.vessels
          .map(
            (vessel) => `
              <button
                class="vessel-selector-chip ${String(vessel.id) === String(state.activeVesselId) ? "active" : ""}"
                type="button"
                data-vessel-select="${vessel.id}"
              >
                <span class="vessel-selector-name">${escapeHtml(vessel.name)}</span>
                <span class="vessel-selector-meta">${escapeHtml(vessel.location || vessel.berth || "No location set")}</span>
              </button>
            `
          )
          .join("")}
        <div class="vessel-selector-actions">
          <button class="text-button vessel-add-button" type="button" data-vessel-action="add">Add vessel</button>
          <button class="text-button vessel-delete-button" type="button" data-vessel-action="delete">Delete vessel</button>
        </div>
      </div>
    </div>
  `;
  const systemCards = [
    {
      label: "Fuel level",
      value: state.vessel.fuel,
      unit: "%",
      description: state.vessel.fuelCapacity
        ? `${formatNumberValue(state.vessel.fuelCapacity)} gal capacity onboard.`
        : "Reserve level for departures, standby, and owner movements.",
      status: vesselLevelStatus("fuel", state.vessel.fuel),
    },
    {
      label: "Water tank",
      value: state.vessel.waterTank,
      unit: "%",
      description: state.vessel.waterCapacity
        ? `${formatNumberValue(state.vessel.waterCapacity)} gal fresh water capacity.`
        : "Fresh water capacity available onboard.",
      status: vesselLevelStatus("water", state.vessel.waterTank),
    },
    {
      label: "Battery bank",
      value: state.vessel.batteryStatus,
      unit: "%",
      description: "Primary battery health and charge picture.",
      status: vesselLevelStatus("fuel", state.vessel.batteryStatus),
    },
    {
      label: "Black water",
      value: state.vessel.blackTankLevel,
      unit: "%",
      description: state.vessel.blackWaterCapacity
        ? `${formatNumberValue(state.vessel.blackWaterCapacity)} gal holding capacity.`
        : "Black water tank level and discharge planning.",
      status: vesselLevelStatus("grey", state.vessel.blackTankLevel),
    },
    {
      label: "Grey tank",
      value: state.vessel.greyTank,
      unit: "%",
      description: state.vessel.greyWaterCapacity
        ? `${formatNumberValue(state.vessel.greyWaterCapacity)} gal grey water capacity.`
        : "Grey water tank fill level and pump-out readiness.",
      status: vesselLevelStatus("grey", state.vessel.greyTank),
    },
    {
      label: "Utilization",
      value: state.vessel.utilization,
      unit: "%",
      description: "Current operational load across trips, service, and guest use.",
      status: vesselLevelStatus("fuel", state.vessel.utilization),
    },
  ];
  const quickSignalCards = [
    {
      label: "Ready board",
      value: `${openTasks}`,
      copy: openTasks ? "Scheduled service items still open." : "No recurring service items are waiting.",
    },
    {
      label: "Weekly workspace",
      value: `${currentWeekEntries}`,
      copy: currentWeekEntries ? "Current Monday-Friday entries are staged in Work Orders." : "No weekly workspace rows have been added yet.",
    },
    {
      label: "Vendor bench",
      value: `${activeVendors}`,
      copy: activeVendors ? "Active vendors already tied to this yacht." : "No active vendors linked yet.",
    },
    {
      label: "Saved reports",
      value: `${reportRows}`,
      copy: reportRows ? "Generated weekly reports already live in this vessel library." : "No weekly reports have been generated yet.",
    },
  ];
  const engineCards = state.vessel.engines.length
    ? state.vessel.engines
        .map(
          (engine) => {
            const serviceState = getMachineryServiceState(engine);
            const nextDueHours = getMachineryNextDueHours(engine);
            const hoursRemaining = getMachineryHoursRemaining(engine);
            return `
            <div class="detail-item machinery-item">
              <div class="machinery-inline-topline">
                <span class="detail-label">${escapeHtml(engine.label)}</span>
                <span class="system-badge ${serviceState.toneClass}">${serviceState.label}</span>
              </div>
              <span class="detail-value">${escapeHtml([engine.manufacturer, engine.model].filter(Boolean).join(" | ") || "Machinery details pending")}</span>
              <span class="detail-caption">${escapeHtml([
                engine.rating,
                engine.hours ? `${formatNumberValue(engine.hours)} hrs` : "",
                nextDueHours !== null ? `Due ${formatNumberValue(nextDueHours)} hrs` : "",
                hoursRemaining !== null ? `${hoursRemaining > 0 ? `${formatNumberValue(hoursRemaining)} hrs left` : `${formatNumberValue(Math.abs(hoursRemaining))} hrs over`}` : "",
                engine.nextServiceDate ? `Next ${formatDate(engine.nextServiceDate)}` : "",
              ].filter(Boolean).join(" | ") || "Add output, hours, and service timing.")}</span>
            </div>
          `;
          }
        )
        .join("")
    : `<div class="empty-state compact-empty-state">No engine records added yet for ${state.vessel.name}.</div>`;
  const generatorCards = state.vessel.generators.length
    ? state.vessel.generators
        .map(
          (generator) => {
            const serviceState = getMachineryServiceState(generator);
            const nextDueHours = getMachineryNextDueHours(generator);
            const hoursRemaining = getMachineryHoursRemaining(generator);
            return `
            <div class="detail-item machinery-item">
              <div class="machinery-inline-topline">
                <span class="detail-label">${escapeHtml(generator.label)}</span>
                <span class="system-badge ${serviceState.toneClass}">${serviceState.label}</span>
              </div>
              <span class="detail-value">${escapeHtml([generator.manufacturer, generator.model].filter(Boolean).join(" | ") || "Machinery details pending")}</span>
              <span class="detail-caption">${escapeHtml([
                generator.rating,
                generator.hours ? `${formatNumberValue(generator.hours)} hrs` : "",
                nextDueHours !== null ? `Due ${formatNumberValue(nextDueHours)} hrs` : "",
                hoursRemaining !== null ? `${hoursRemaining > 0 ? `${formatNumberValue(hoursRemaining)} hrs left` : `${formatNumberValue(Math.abs(hoursRemaining))} hrs over`}` : "",
                generator.nextServiceDate ? `Next ${formatDate(generator.nextServiceDate)}` : "",
              ].filter(Boolean).join(" | ") || "Add output, hours, and service timing.")}</span>
            </div>
          `;
          }
        )
        .join("")
    : `<div class="empty-state compact-empty-state">No generator records added yet for ${state.vessel.name}.</div>`;

  elements.vesselDashboard.innerHTML = `
    ${selectorMarkup}
    <article class="vessel-profile-hero">
      <div class="vessel-profile-media ${state.vessel.photoDataUrl ? "" : "is-empty"}">
        ${
          state.vessel.photoDataUrl
            ? `<img src="${escapeHtml(state.vessel.photoDataUrl)}" alt="${escapeHtml(state.vessel.name)} profile" />`
            : `
                <div class="vessel-profile-placeholder">
                  <span class="metric-label">Vessel photo</span>
                  <p class="small-copy">Upload a main image to give this yacht a proper profile header.</p>
                </div>
              `
        }
      </div>
      <div class="vessel-profile-copy">
        <p class="eyebrow">Active Vessel</p>
        <div class="vessel-profile-title-row">
          <div>
            <h2>${escapeHtml(state.vessel.name)}</h2>
            <p class="vessel-profile-subtitle">${escapeHtml([state.vessel.yearBuilt || "", state.vessel.builder, state.vessel.model].filter(Boolean).join(" | ") || "Builder and model pending")}</p>
          </div>
          <span class="status-badge ${statusClass(state.vessel.status)}">${state.vessel.status}</span>
        </div>
        <div class="vessel-profile-pills">
          <span class="vessel-profile-pill">Type ${escapeHtml(state.vessel.vesselType || "Pending")}</span>
          <span class="vessel-profile-pill">Captain ${escapeHtml(state.vessel.captain || "Unassigned")}</span>
          <span class="vessel-profile-pill">Location ${escapeHtml(state.vessel.location || "Unknown")}</span>
          <span class="vessel-profile-pill">Berth ${escapeHtml(state.vessel.berth || "Open")}</span>
          <span class="vessel-profile-pill">Guests ${escapeHtml(String(state.vessel.guests || 0))}</span>
        </div>
        <p class="vessel-profile-note">${escapeHtml(state.vessel.notes || "Add operating notes, owner preferences, and standing details for this vessel.")}</p>
        <div class="vessel-quick-actions">
          <button class="primary-button vessel-action-button" type="button" data-vessel-scroll="#vessel-form-section">Edit profile</button>
          <button class="ghost-button vessel-action-button" type="button" data-vessel-scroll="#vessel-systems-section">Systems watch</button>
          <button class="ghost-button vessel-action-button" type="button" data-vessel-scroll="#vessel-engine-section">Engines</button>
          <button class="ghost-button vessel-action-button" type="button" data-vessel-scroll="#vessel-generator-section">Generators</button>
        </div>
        <div class="vessel-profile-stats">
          <article class="vessel-stat-card">
            <span class="metric-label">Length</span>
            <strong>${escapeHtml(formatNumberValue(state.vessel.length))} ft</strong>
          </article>
          <article class="vessel-stat-card">
            <span class="metric-label">Beam</span>
            <strong>${escapeHtml(formatNumberValue(state.vessel.beam))} ft</strong>
          </article>
          <article class="vessel-stat-card">
            <span class="metric-label">Draft</span>
            <strong>${escapeHtml(formatNumberValue(state.vessel.draft))} ft</strong>
          </article>
          <article class="vessel-stat-card">
            <span class="metric-label">Next service</span>
            <strong>${escapeHtml(formatDate(state.vessel.nextService))}</strong>
          </article>
        </div>
        <div class="vessel-glance-grid">
          ${quickSignalCards
            .map(
              (card) => `
                <article class="vessel-glance-card">
                  <span class="detail-label">${card.label}</span>
                  <strong>${escapeHtml(card.value)}</strong>
                  <p class="small-copy">${escapeHtml(card.copy)}</p>
                </article>
              `
            )
            .join("")}
        </div>
      </div>
    </article>
    <div class="vessel-widget-grid">
    ${systemCards
      .map(
        (card) => `
          <article class="vessel-system-card">
            <span class="metric-label">${card.label}</span>
            <div class="system-value-row">
              <span class="metric-value">${card.value}${card.unit || ""}</span>
              <span class="system-badge ${card.status.toneClass}">${card.status.label}</span>
            </div>
            <p class="small-copy system-copy">${card.description}</p>
            <div class="level-track">
              <div class="level-fill ${card.status.fillClass}" style="width: ${Math.max(card.value, 6)}%"></div>
            </div>
          </article>
        `
      )
      .join("")}
    </div>
    <div class="vessel-details-grid">
    <article class="vessel-system-card vessel-system-card-wide vessel-detail-card">
      <div class="card-topline">
        <div>
          <span class="metric-label">Vessel specs</span>
          <h3 class="card-title">${state.vessel.name} at a glance</h3>
        </div>
        <span class="status-badge ${statusClass(state.vessel.status)}">${state.vessel.status}</span>
      </div>
      <div class="system-meta-grid">
        <div class="detail-item">
          <span class="detail-label">Fuel capacity</span>
          <span class="detail-value">${escapeHtml(formatNumberValue(state.vessel.fuelCapacity))} gal</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Water capacity</span>
          <span class="detail-value">${escapeHtml(formatNumberValue(state.vessel.waterCapacity))} gal</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Black water cap.</span>
          <span class="detail-value">${escapeHtml(formatNumberValue(state.vessel.blackWaterCapacity))} gal</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Grey water cap.</span>
          <span class="detail-value">${escapeHtml(formatNumberValue(state.vessel.greyWaterCapacity))} gal</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Location</span>
          <span class="detail-value">${escapeHtml(state.vessel.location || "Unknown")}</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Year built</span>
          <span class="detail-value">${escapeHtml(state.vessel.yearBuilt ? String(state.vessel.yearBuilt) : "Pending")}</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Hull material</span>
          <span class="detail-value">${escapeHtml(state.vessel.hullMaterial || "Pending")}</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Berth</span>
          <span class="detail-value">${escapeHtml(state.vessel.berth || "Open")}</span>
        </div>
      </div>
      <p class="system-note">This profile is tied to its own maintenance, work orders, vendors, expenses, reports, inventory, crew, and voyage records.</p>
    </article>
    <article class="vessel-system-card vessel-detail-card">
      <div class="card-topline">
        <div>
          <span class="metric-label">Machinery</span>
          <h3 class="card-title">Engines and generation</h3>
        </div>
      </div>
      <div class="vessel-machinery-stack">
        ${engineCards}
        ${generatorCards}
        <div class="detail-item">
          <span class="detail-label">Battery status</span>
          <span class="detail-value">${escapeHtml(String(state.vessel.batteryStatus))}%</span>
        </div>
      </div>
    </article>
    <article class="vessel-system-card vessel-system-card-wide vessel-detail-card">
      <div class="card-topline">
        <div>
          <span class="metric-label">Service pulse</span>
          <h3 class="card-title">How this vessel is trending right now</h3>
        </div>
      </div>
      <div class="system-meta-grid vessel-service-grid">
        <div class="detail-item">
          <span class="detail-label">Recurring maintenance</span>
          <span class="detail-value">${escapeHtml(String(openTasks))} open</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Weekly workspace</span>
          <span class="detail-value">${escapeHtml(String(currentWeekEntries))} entries</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Vendor directory</span>
          <span class="detail-value">${escapeHtml(String(state.vendors.length))} contacts</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Saved reports</span>
          <span class="detail-value">${escapeHtml(String(reportRows))} reports</span>
        </div>
      </div>
      <p class="system-note">Use the profile editor for public-facing vessel details and the systems editor for operational numbers, capacities, and machinery notes.</p>
    </article>
    </div>
  `;
}

function populateVesselSystemsForm() {
  const fields = elements.vesselSystemsForm.elements;

  fields.namedItem("fuel").value = state.vessel.fuel;
  fields.namedItem("waterTank").value = state.vessel.waterTank;
  fields.namedItem("greyTank").value = state.vessel.greyTank;
  fields.namedItem("blackTankLevel").value = state.vessel.blackTankLevel;
  fields.namedItem("batteryStatus").value = state.vessel.batteryStatus;
  fields.namedItem("utilization").value = state.vessel.utilization;
  fields.namedItem("fuelCapacity").value = state.vessel.fuelCapacity;
  fields.namedItem("waterCapacity").value = state.vessel.waterCapacity;
  fields.namedItem("greyWaterCapacity").value = state.vessel.greyWaterCapacity;
  fields.namedItem("blackWaterCapacity").value = state.vessel.blackWaterCapacity;
  fields.namedItem("nextService").value = state.vessel.nextService;
  fields.namedItem("engineInfo").value = state.vessel.engineInfo || "";
  fields.namedItem("generatorInfo").value = state.vessel.generatorInfo || "";
}

function renderEngineList() {
  if (!state.vessel.engines.length) {
    elements.engineList.innerHTML = `<div class="empty-state">No engine records logged for ${state.vessel.name} yet.</div>`;
    return;
  }

  elements.engineList.innerHTML = state.vessel.engines
    .slice()
    .sort((a, b) => String(a.label || "").localeCompare(String(b.label || "")))
    .map((engine) => {
      const serviceState = getMachineryServiceState(engine);
      const nextDueHours = getMachineryNextDueHours(engine);
      const hoursRemaining = getMachineryHoursRemaining(engine);
      return `
        <article class="machinery-card">
          <div class="card-topline">
            <div>
              <h3 class="card-title">${escapeHtml(engine.label)}</h3>
              <p class="card-meta">${escapeHtml([engine.manufacturer, engine.model, engine.rating].filter(Boolean).join(" | ") || "Engine details pending")}</p>
            </div>
            <span class="status-badge ${serviceState.statusClass}">
              ${serviceState.label}
            </span>
          </div>
          <div class="machinery-card-grid">
            <div class="detail-item">
              <span class="detail-label">Hours</span>
              <span class="detail-value">${escapeHtml(formatNumberValue(engine.hours))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Last service hrs</span>
              <span class="detail-value">${escapeHtml(formatNumberValue(engine.lastServiceHours))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Interval</span>
              <span class="detail-value">${escapeHtml(formatNumberValue(engine.serviceIntervalHours))} hrs</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Due hours</span>
              <span class="detail-value">${escapeHtml(nextDueHours !== null ? formatNumberValue(nextDueHours) : "N/A")}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Hours remaining</span>
              <span class="detail-value">${escapeHtml(hoursRemaining !== null ? String(hoursRemaining) : "N/A")}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Last service</span>
              <span class="detail-value">${escapeHtml(formatDate(engine.lastServiceDate))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Next service</span>
              <span class="detail-value">${escapeHtml(formatDate(engine.nextServiceDate))}</span>
            </div>
          </div>
          <p class="small-copy">${escapeHtml(engine.notes || "No service notes logged yet.")}</p>
          <div class="vendor-actions">
            <button class="table-action action-edit" type="button" data-engine-action="edit" data-engine-id="${engine.id}">Edit</button>
            <button class="table-action action-delete" type="button" data-engine-action="delete" data-engine-id="${engine.id}">Delete</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderGeneratorList() {
  if (!state.vessel.generators.length) {
    elements.generatorList.innerHTML = `<div class="empty-state">No generator records logged for ${state.vessel.name} yet.</div>`;
    return;
  }

  elements.generatorList.innerHTML = state.vessel.generators
    .slice()
    .sort((a, b) => String(a.label || "").localeCompare(String(b.label || "")))
    .map((generator) => {
      const serviceState = getMachineryServiceState(generator);
      const nextDueHours = getMachineryNextDueHours(generator);
      const hoursRemaining = getMachineryHoursRemaining(generator);
      return `
        <article class="machinery-card">
          <div class="card-topline">
            <div>
              <h3 class="card-title">${escapeHtml(generator.label)}</h3>
              <p class="card-meta">${escapeHtml([generator.manufacturer, generator.model, generator.rating].filter(Boolean).join(" | ") || "Generator details pending")}</p>
            </div>
            <span class="status-badge ${serviceState.statusClass}">
              ${serviceState.label}
            </span>
          </div>
          <div class="machinery-card-grid">
            <div class="detail-item">
              <span class="detail-label">Hours</span>
              <span class="detail-value">${escapeHtml(formatNumberValue(generator.hours))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Last service hrs</span>
              <span class="detail-value">${escapeHtml(formatNumberValue(generator.lastServiceHours))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Interval</span>
              <span class="detail-value">${escapeHtml(formatNumberValue(generator.serviceIntervalHours))} hrs</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Due hours</span>
              <span class="detail-value">${escapeHtml(nextDueHours !== null ? formatNumberValue(nextDueHours) : "N/A")}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Hours remaining</span>
              <span class="detail-value">${escapeHtml(hoursRemaining !== null ? String(hoursRemaining) : "N/A")}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Last service</span>
              <span class="detail-value">${escapeHtml(formatDate(generator.lastServiceDate))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Next service</span>
              <span class="detail-value">${escapeHtml(formatDate(generator.nextServiceDate))}</span>
            </div>
          </div>
          <p class="small-copy">${escapeHtml(generator.notes || "No service notes logged yet.")}</p>
          <div class="vendor-actions">
            <button class="table-action action-edit" type="button" data-generator-action="edit" data-generator-id="${generator.id}">Edit</button>
            <button class="table-action action-delete" type="button" data-generator-action="delete" data-generator-id="${generator.id}">Delete</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderEngineForm() {
  const fields = elements.engineForm.elements;
  const engine = state.vessel.engines.find((item) => item.id === editingEngineId) || null;

  if (!engine) {
    if (editingEngineId) {
      editingEngineId = null;
    }
    elements.engineForm.reset();
    elements.engineForm.elements.namedItem("hours").value = "0";
    elements.engineForm.elements.namedItem("lastServiceHours").value = "0";
    elements.engineForm.elements.namedItem("serviceIntervalHours").value = "0";
    elements.engineSubmit.textContent = "Add engine";
    elements.engineCancel.hidden = true;
    return;
  }

  fields.namedItem("label").value = engine.label;
  fields.namedItem("manufacturer").value = engine.manufacturer || "";
  fields.namedItem("model").value = engine.model || "";
  fields.namedItem("rating").value = engine.rating || "";
  fields.namedItem("hours").value = String(engine.hours ?? 0);
  fields.namedItem("lastServiceHours").value = String(engine.lastServiceHours ?? 0);
  fields.namedItem("serviceIntervalHours").value = String(engine.serviceIntervalHours ?? 0);
  fields.namedItem("lastServiceDate").value = engine.lastServiceDate || "";
  fields.namedItem("nextServiceDate").value = engine.nextServiceDate || "";
  fields.namedItem("notes").value = engine.notes || "";
  elements.engineSubmit.textContent = "Save changes";
  elements.engineCancel.hidden = false;
}

function renderGeneratorForm() {
  const fields = elements.generatorForm.elements;
  const generator = state.vessel.generators.find((item) => item.id === editingGeneratorId) || null;

  if (!generator) {
    if (editingGeneratorId) {
      editingGeneratorId = null;
    }
    elements.generatorForm.reset();
    elements.generatorForm.elements.namedItem("hours").value = "0";
    elements.generatorForm.elements.namedItem("lastServiceHours").value = "0";
    elements.generatorForm.elements.namedItem("serviceIntervalHours").value = "0";
    elements.generatorSubmit.textContent = "Add generator";
    elements.generatorCancel.hidden = true;
    return;
  }

  fields.namedItem("label").value = generator.label;
  fields.namedItem("manufacturer").value = generator.manufacturer || "";
  fields.namedItem("model").value = generator.model || "";
  fields.namedItem("rating").value = generator.rating || "";
  fields.namedItem("hours").value = String(generator.hours ?? 0);
  fields.namedItem("lastServiceHours").value = String(generator.lastServiceHours ?? 0);
  fields.namedItem("serviceIntervalHours").value = String(generator.serviceIntervalHours ?? 0);
  fields.namedItem("lastServiceDate").value = generator.lastServiceDate || "";
  fields.namedItem("nextServiceDate").value = generator.nextServiceDate || "";
  fields.namedItem("notes").value = generator.notes || "";
  elements.generatorSubmit.textContent = "Save changes";
  elements.generatorCancel.hidden = false;
}

function renderSpotlight() {
  const nextCharter = state.charters.slice().sort((a, b) => a.start.localeCompare(b.start))[0];
  const openTasks = state.maintenance.filter((task) => !isMaintenanceComplete(task)).length;
  const currentWeekEntries = getCurrentWeekWorkOrders().length;
  const vesselPhotoMarkup = state.vessel.photoDataUrl
    ? `
        <div class="spotlight-photo-shell">
          <img class="spotlight-vessel-photo" src="${escapeHtml(state.vessel.photoDataUrl)}" alt="${escapeHtml(
            state.vessel.name
          )} profile" />
        </div>
      `
    : `
        <div class="spotlight-photo-shell spotlight-photo-empty">
          <span class="metric-label">Vessel photo</span>
          <p class="small-copy">Upload a yacht image from the Vessel tab and it will appear here.</p>
        </div>
      `;

  elements.spotlightPanel.innerHTML = `
    <div class="spotlight-grid">
      <article class="spotlight-banner">
        <div class="spotlight-title">
          <p class="eyebrow">Active Vessel</p>
          <h2>${state.vessel.name}</h2>
          <span class="status-badge ${statusClass(state.vessel.status)}">${state.vessel.status}</span>
        </div>
        <p class="spotlight-description">${state.vessel.notes}</p>
        <div class="inline-metrics">
          <div class="metric-tile">
            <span class="metric-label">Fuel reserve</span>
            <span class="metric-value">${state.vessel.fuel}%</span>
          </div>
          <div class="metric-tile">
            <span class="metric-label">Water tank</span>
            <span class="metric-value">${state.vessel.waterTank}%</span>
          </div>
          <div class="metric-tile">
            <span class="metric-label">Grey tank</span>
            <span class="metric-value">${state.vessel.greyTank}%</span>
          </div>
        </div>
        ${vesselPhotoMarkup}
      </article>

      <div class="spotlight-side">
        <article class="progress-card">
          <span class="metric-label">Next service window</span>
          <span class="metric-value">${formatDate(state.vessel.nextService)}</span>
          <p class="small-copy">${openTasks} open maintenance tasks and ${currentWeekEntries} weekly workspace entries.</p>
          <div class="progress-track">
            <div class="progress-fill" style="width: ${Math.max(18, state.vessel.fuel)}%"></div>
          </div>
        </article>

        <article class="note-card weather-card">
          ${renderWeatherCard()}
        </article>

        <article class="note-card">
          <span class="metric-label">Command snapshot</span>
          <div class="detail-row">
            <div class="detail-item">
              <span class="detail-label">Captain</span>
              <span class="detail-value">${state.vessel.captain}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Location</span>
              <span class="detail-value">${state.vessel.location}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Guests</span>
              <span class="detail-value">${state.vessel.guests}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Next booking</span>
              <span class="detail-value">${nextCharter ? formatDate(nextCharter.start) : "Unscheduled"}</span>
            </div>
          </div>
        </article>
      </div>
    </div>
  `;
}

function renderOverview() {
  const nextCharter = state.charters.slice().sort((a, b) => a.start.localeCompare(b.start))[0];
  const recentReports = getAllReportEntries()
    .slice()
    .sort((a, b) => b.reportDate.localeCompare(a.reportDate))
    .slice(0, 3);
  const openTasks = state.maintenance.filter((task) => !isMaintenanceComplete(task));
  const currentWeekEntries = getCurrentWeekWorkOrders();
  const weeklyIssues = currentWeekEntries.filter((entry) => String(entry.issues || "").trim());
  const priorityQueueItems = weeklyIssues.length ? weeklyIssues : openTasks;
  const priorityQueueView = weeklyIssues.length ? "work-orders" : "maintenance";

  elements.overviewPanel.innerHTML = `
    <div class="section-heading">
      <div>
        <p class="eyebrow">Bridge View</p>
        <h2>Operations overview</h2>
      </div>
      <span class="status-badge ${statusClass(state.vessel.status)}">${state.vessel.name}</span>
    </div>

    <div class="overview-grid">
      <article class="overview-card overview-card-feature">
        <span class="metric-label">Today's focus</span>
        <h3 class="card-title">${state.vessel.name} command brief</h3>
        <p class="report-copy">${state.vessel.notes}</p>
      </article>

      <article class="overview-card">
        <span class="metric-label">Operations snapshot</span>
        <div class="overview-stat-list">
          <button class="overview-stat overview-link" type="button" data-view-target="maintenance">
            <span class="detail-label">Open maintenance</span>
            <span class="detail-value">${openTasks.length}</span>
          </button>
          <button class="overview-stat overview-link" type="button" data-view-target="work-orders">
            <span class="detail-label">Weekly workspace</span>
            <span class="detail-value">${currentWeekEntries.length}</span>
          </button>
          <button class="overview-stat overview-link" type="button" data-view-target="crew">
            <span class="detail-label">Crew assigned</span>
            <span class="detail-value">${state.crew.length}</span>
          </button>
          <button class="overview-stat overview-link" type="button" data-view-target="reports">
            <span class="detail-label">Saved reports</span>
            <span class="detail-value">${state.reports.length}</span>
          </button>
        </div>
      </article>

      <article class="overview-card">
        <span class="metric-label">Recent weekly entries</span>
        <div class="overview-list">
          ${
            recentReports.length
              ? recentReports
                  .map(
                    (report) => `
                      <button class="overview-list-item overview-link" type="button" data-view-target="reports" data-report-id="${escapeHtml(report.reportId)}">
                        <strong>${escapeHtml(report.item)} | ${formatReportDate(report.reportDate)}</strong>
                        <p class="small-copy">${escapeHtml(report.workDone)}</p>
                      </button>
                    `
                  )
                  .join("")
              : `<div class="empty-state">No generated weekly report entries have been saved for ${state.vessel.name} yet.</div>`
          }
        </div>
      </article>

      <article class="overview-card">
        <span class="metric-label">Priority queue</span>
        <div class="overview-list">
          ${
            priorityQueueItems.length
              ? priorityQueueItems
                  .slice(0, 3)
                  .map(
                    (task) => `
                      <button class="overview-list-item overview-link" type="button" data-view-target="${priorityQueueView}">
                        <strong>${escapeHtml(task.item || task.title)}</strong>
                        <p class="small-copy">${
                          priorityQueueView === "work-orders"
                            ? `${formatDate(task.reportDate || task.dueDate)} | ${escapeHtml(task.issues || task.workDone || task.notes || "Weekly workspace item")}`
                            : `Due ${formatDate(task.dueDate)} | ${escapeHtml(task.notes || "Maintenance task")}`
                        }</p>
                      </button>
                    `
                  )
                  .join("")
              : `<div class="empty-state">No active maintenance tasks or weekly issues for ${state.vessel.name}.</div>`
          }
        </div>
      </article>
    </div>
  `;
}

function renderWeatherCard() {
  const actionLabel = weatherState.status === "loading" ? "Refreshing..." : "Refresh";
  const actionButton = `
    <button
      class="text-button weather-refresh"
      type="button"
      data-weather-action="refresh"
      ${weatherState.status === "loading" ? "disabled" : ""}
    >
      ${actionLabel}
    </button>
  `;

  if (weatherState.status === "loading" && !weatherState.data) {
    return `
      <div class="weather-card-header">
        <div>
          <span class="metric-label">Local weather</span>
          <h3 class="card-title">Checking your location</h3>
        </div>
        ${actionButton}
      </div>
      <p class="small-copy weather-copy">Loading current conditions for your area.</p>
    `;
  }

  if (weatherState.status === "error" && !weatherState.data) {
    return `
      <div class="weather-card-header">
        <div>
          <span class="metric-label">Local weather</span>
          <h3 class="card-title">Weather unavailable</h3>
        </div>
        ${actionButton}
      </div>
      <p class="small-copy weather-copy">${weatherState.error}</p>
    `;
  }

  if (!weatherState.data) {
    return `
      <div class="weather-card-header">
        <div>
          <span class="metric-label">Local weather</span>
          <h3 class="card-title">Waiting for update</h3>
        </div>
        ${actionButton}
      </div>
      <p class="small-copy weather-copy">Use refresh to load local weather.</p>
    `;
  }

  return `
    <div class="weather-card-header">
      <div>
        <span class="metric-label">Local weather</span>
        <h3 class="card-title">${escapeHtml(weatherState.label)}</h3>
      </div>
      ${actionButton}
    </div>
    <div class="weather-reading">
      <div>
        <div class="weather-temp">${Math.round(weatherState.data.temperature)}°F</div>
        <p class="small-copy weather-copy">${weatherState.data.condition}</p>
      </div>
      <div class="weather-grid">
        <div class="weather-stat">
          <span class="detail-label">High / Low</span>
          <span class="detail-value">${Math.round(weatherState.data.high)}° / ${Math.round(weatherState.data.low)}°</span>
        </div>
        <div class="weather-stat">
          <span class="detail-label">Wind</span>
          <span class="detail-value">${Math.round(weatherState.data.windSpeed)} kn</span>
        </div>
      </div>
    </div>
    <p class="small-copy weather-meta">
      Updated ${formatWeatherUpdated(weatherState.data.updatedAt)}${weatherState.source === "vessel" ? ` using ${escapeHtml(state.vessel.location)}` : ""}
    </p>
  `;
}

function createDefaultWeatherState() {
  return {
    status: "idle",
    source: "",
    label: "Current location",
    data: null,
    error: "",
    lastFetched: 0,
  };
}

async function ensureWeatherData({ force = false } = {}) {
  const isFresh = weatherState.status === "ready" && Date.now() - weatherState.lastFetched < WEATHER_REFRESH_MS;
  if (!force && (weatherState.status === "loading" || isFresh)) {
    return;
  }

  weatherState = {
    ...weatherState,
    status: "loading",
    error: "",
  };
  renderSpotlight();

  try {
    let nextWeatherState;

    try {
      const position = await getBrowserPosition();
      nextWeatherState = await fetchWeatherForCoordinates(
        position.coords.latitude,
        position.coords.longitude,
        "Current location",
        "user"
      );
    } catch (locationError) {
      if (!state.vessel.location) {
        throw locationError;
      }

      nextWeatherState = await fetchWeatherForPlace(state.vessel.location);
    }

    weatherState = {
      ...weatherState,
      ...nextWeatherState,
      status: "ready",
      error: "",
      lastFetched: Date.now(),
    };
  } catch (error) {
    weatherState = {
      ...weatherState,
      status: weatherState.data ? "ready" : "error",
      error: "Location-based weather is unavailable right now.",
    };
  }

  renderSpotlight();
}

function getBrowserPosition() {
  return new Promise((resolve, reject) => {
    if (!("geolocation" in navigator)) {
      reject(new Error("Geolocation is unavailable."));
      return;
    }

    navigator.geolocation.getCurrentPosition(resolve, reject, {
      enableHighAccuracy: false,
      timeout: 10000,
      maximumAge: WEATHER_REFRESH_MS,
    });
  });
}

async function fetchWeatherForPlace(place) {
  const params = new URLSearchParams({
    name: place,
    count: "1",
    language: "en",
    format: "json",
  });
  const response = await fetch(`https://geocoding-api.open-meteo.com/v1/search?${params.toString()}`);

  if (!response.ok) {
    throw new Error("Weather geocoding failed.");
  }

  const payload = await response.json();
  const result = payload.results?.[0];

  if (!result) {
    throw new Error("No weather location match found.");
  }

  return fetchWeatherForCoordinates(result.latitude, result.longitude, result.name, "vessel");
}

async function fetchWeatherForCoordinates(latitude, longitude, label, source) {
  const params = new URLSearchParams({
    latitude: String(latitude),
    longitude: String(longitude),
    current_weather: "true",
    daily: "temperature_2m_max,temperature_2m_min",
    temperature_unit: "fahrenheit",
    windspeed_unit: "kn",
    timezone: "auto",
    forecast_days: "1",
  });
  const response = await fetch(`https://api.open-meteo.com/v1/forecast?${params.toString()}`);

  if (!response.ok) {
    throw new Error("Weather forecast failed.");
  }

  const payload = await response.json();
  const currentWeather = payload.current_weather;

  if (!currentWeather || !payload.daily) {
    throw new Error("Weather data is incomplete.");
  }

  return {
    source,
    label,
    data: {
      temperature: currentWeather.temperature,
      windSpeed: currentWeather.windspeed,
      condition: weatherCodeLabel(currentWeather.weathercode),
      high: payload.daily.temperature_2m_max?.[0] ?? currentWeather.temperature,
      low: payload.daily.temperature_2m_min?.[0] ?? currentWeather.temperature,
      updatedAt: currentWeather.time,
    },
  };
}

function weatherCodeLabel(code) {
  if (code === 0) {
    return "Clear";
  }

  if ([1, 2, 3].includes(code)) {
    return "Partly cloudy";
  }

  if ([45, 48].includes(code)) {
    return "Fog";
  }

  if ([51, 53, 55, 56, 57].includes(code)) {
    return "Drizzle";
  }

  if ([61, 63, 65, 66, 67, 80, 81, 82].includes(code)) {
    return "Rain";
  }

  if ([71, 73, 75, 77, 85, 86].includes(code)) {
    return "Snow";
  }

  if ([95, 96, 99].includes(code)) {
    return "Thunderstorm";
  }

  return "Mixed conditions";
}

function renderMaintenanceFocus(reminderTasks, overdueTasks, dueTodayTasks, completedThisWeek, totalTasks = state.maintenance.length) {
  const dueSoonTasks = reminderTasks.filter((task) => !isMaintenanceOverdue(task));
  const nextAttentionTask = overdueTasks[0] || dueTodayTasks[0] || dueSoonTasks[0] || null;
  const healthState = overdueTasks.length
    ? {
        label: "Overdue",
        toneClass: "is-danger",
        summary: `${escapeHtml(overdueTasks[0].title)} needs attention first.`,
      }
    : dueTodayTasks.length || dueSoonTasks.length
      ? {
          label: "Attention",
          toneClass: "is-warn",
          summary: nextAttentionTask
            ? `${escapeHtml(nextAttentionTask.title)} is the next service window to handle.`
            : "A few service cycles are approaching.",
        }
      : {
          label: "Healthy",
          toneClass: "is-good",
          summary: "No overdue work and no active reminders right now.",
        };

  return `
    <article class="maintenance-summary-card maintenance-summary-card-health ${healthState.toneClass}">
      <div class="maintenance-summary-topline">
        <span class="metric-label">Vessel health</span>
        <span class="maintenance-inline-hint ${healthState.toneClass}">${healthState.label}</span>
      </div>
      <strong class="maintenance-summary-value">${healthState.label}</strong>
      <p class="small-copy maintenance-summary-copy">${healthState.summary}</p>
    </article>
    <article class="maintenance-summary-card">
      <span class="metric-label">Total active tasks</span>
      <strong class="maintenance-summary-value">${totalTasks}</strong>
      <p class="small-copy maintenance-summary-copy">Recurring service items currently tracked on this vessel.</p>
    </article>
    <article class="maintenance-summary-card">
      <span class="metric-label">Due soon</span>
      <strong class="maintenance-summary-value">${dueSoonTasks.length + dueTodayTasks.length}</strong>
      <p class="small-copy maintenance-summary-copy">${
        nextAttentionTask
          ? `${escapeHtml(nextAttentionTask.title)} is next in line.`
          : "Nothing is approaching its reminder window."
      }</p>
    </article>
    <article class="maintenance-summary-card">
      <span class="metric-label">Overdue</span>
      <strong class="maintenance-summary-value">${overdueTasks.length}</strong>
      <p class="small-copy maintenance-summary-copy">${
        overdueTasks.length
          ? `${escapeHtml(overdueTasks[0].title)} is already past due.`
          : "No overdue service cycles."
      }</p>
    </article>
    <article class="maintenance-summary-card">
      <span class="metric-label">Completed recently</span>
      <strong class="maintenance-summary-value">${completedThisWeek.length}</strong>
      <p class="small-copy maintenance-summary-copy">${
        completedThisWeek.length
          ? `${escapeHtml(getMaintenanceHistoryTaskLabel(completedThisWeek[0]))} was completed most recently.`
          : "No completions logged in the last 7 days."
      }</p>
    </article>
  `;
}

function renderMaintenanceOverviewSpotlight(tasks, reminderTasks, overdueTasks, dueTodayTasks, completedThisWeek) {
  const dueSoonTasks = reminderTasks.filter((task) => !isMaintenanceOverdue(task));
  const nextAttentionTask = overdueTasks[0] || dueTodayTasks[0] || dueSoonTasks[0] || null;
  const recentCompletion = completedThisWeek[0] || null;

  if (!tasks.length && !state.maintenanceAssets.length) {
    return `
      <div class="maintenance-empty-state">
        <span class="metric-label">Quick start</span>
        <h4>Set up your vessel maintenance</h4>
        <p class="small-copy">Harbor Command can automatically generate recurring service from your vessel systems.</p>
        ${
          isOwnerReadOnly()
            ? '<p class="small-copy maintenance-owner-note">This vessel has not been configured yet. A captain or management user can install equipment and starter templates.</p>'
            : `
              <div class="maintenance-empty-actions">
                <button class="primary-button" type="button" data-maintenance-quick-action="starter-pack">Apply Starter Maintenance Pack</button>
                <button class="ghost-button maintenance-ghost-button" type="button" data-maintenance-quick-action="add-equipment">Install Individual Equipment</button>
                <button class="text-button" type="button" data-maintenance-quick-action="start-scratch">Start from scratch</button>
              </div>
            `
        }
      </div>
    `;
  }

  return `
    <div class="maintenance-overview-grid">
      <article class="maintenance-focus-card maintenance-focus-card-wide">
        <div class="maintenance-focus-topline">
          <div>
            <p class="eyebrow">What needs attention right now?</p>
            <h3 class="card-title">${
              nextAttentionTask
                ? escapeHtml(nextAttentionTask.title)
                : "The vessel is in a healthy maintenance window"
            }</h3>
          </div>
          ${
            nextAttentionTask
              ? `<span class="maintenance-inline-hint ${getMaintenanceDueHint(nextAttentionTask).toneClass}">${escapeHtml(getMaintenanceDueHint(nextAttentionTask).label)}</span>`
              : '<span class="maintenance-inline-hint is-good">Healthy</span>'
          }
        </div>
        <p class="maintenance-focus-copy">${
          nextAttentionTask
            ? escapeHtml(nextAttentionTask.notes || getMaintenanceDueHint(nextAttentionTask).detail || "This task is the next service item that needs attention.")
            : "No overdue work is blocking the vessel right now. Keep an eye on the due-soon list and recent completions."
        }</p>
        <div class="detail-row">
          <div class="detail-item">
            <span class="detail-label">System</span>
            <span class="detail-value">${escapeHtml(nextAttentionTask?.category || "All systems")}</span>
          </div>
          <div class="detail-item">
            <span class="detail-label">Next due</span>
            <span class="detail-value">${escapeHtml(nextAttentionTask ? formatMaintenanceDueValue(nextAttentionTask) : "No urgent due date")}</span>
          </div>
        </div>
      </article>
      <article class="maintenance-focus-card">
        <span class="metric-label">Due soon queue</span>
        <strong class="maintenance-focus-value">${dueSoonTasks.length + dueTodayTasks.length}</strong>
        <p class="maintenance-focus-copy">${
          dueSoonTasks.length || dueTodayTasks.length
            ? `${escapeHtml((dueTodayTasks[0] || dueSoonTasks[0]).title)} is the next service window to prepare for.`
            : "Nothing is entering the reminder window right now."
        }</p>
      </article>
      <article class="maintenance-focus-card">
        <span class="metric-label">Completed recently</span>
        <strong class="maintenance-focus-value">${completedThisWeek.length}</strong>
        <p class="maintenance-focus-copy">${
          recentCompletion
            ? `${escapeHtml(getMaintenanceHistoryTaskLabel(recentCompletion))} completed ${formatOptionalShortDate(recentCompletion.completionDate)}.`
            : "No service completions logged in the last 7 days."
        }</p>
      </article>
    </div>
  `;
}

function renderMaintenanceWorkspacePanels() {
  const ownerMode = isOwnerReadOnly();
  const desiredWorkspace = ownerMode && state.activeMaintenanceWorkspace === "new-service-task"
    ? "overview"
    : state.activeMaintenanceWorkspace;

  state.activeMaintenanceWorkspace = desiredWorkspace;

  elements.maintenanceSubtabs.forEach((tab) => {
    const view = tab.dataset.maintenanceView;
    const hidden = ownerMode && view === "new-service-task";
    const isActive = !hidden && view === desiredWorkspace;
    tab.hidden = hidden;
    tab.classList.toggle("active", isActive);
    tab.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  elements.maintenanceWorkspacePanels.forEach((panel) => {
    const isActive = panel.dataset.maintenancePanel === desiredWorkspace;
    panel.classList.toggle("active", isActive);
    panel.hidden = !isActive;
  });
}

function activateMaintenanceWorkspace(nextWorkspace, options = {}) {
  const { persist = true, focusSelector = "" } = options;
  if (!MAINTENANCE_WORKSPACE_VIEWS.includes(nextWorkspace)) {
    return;
  }

  state.activeView = "maintenance";
  state.activeMaintenanceWorkspace = nextWorkspace;

  if (persist) {
    persistAndRender();
  } else {
    renderApp();
  }

  const focusTarget = focusSelector ? document.querySelector(focusSelector) : null;
  const fallbackTarget = elements.maintenanceSubnav?.closest(".maintenance-workspace-shell");
  const scrollTarget = focusTarget || fallbackTarget;

  if (scrollTarget) {
    window.requestAnimationFrame(() => {
      scrollTarget.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }
}

function renderMaintenanceSections(categories, tasks) {
  const counts = new Map();
  tasks.forEach((task) => {
    counts.set(task.category, (counts.get(task.category) || 0) + 1);
  });

  return `
    <button
      class="maintenance-section-chip ${state.activeMaintenanceCategory === "all" ? "active" : ""}"
      type="button"
      data-maintenance-category="all"
    >
      <span>All systems</span>
      <strong>${tasks.length}</strong>
    </button>
    ${categories
      .map(
        (category) => `
          <button
            class="maintenance-section-chip ${state.activeMaintenanceCategory === category ? "active" : ""}"
            type="button"
            data-maintenance-category="${escapeHtml(category)}"
          >
            <span>${escapeHtml(category)}</span>
            <strong>${counts.get(category) || 0}</strong>
          </button>
        `
      )
      .join("")}
  `;
}

function renderMaintenanceSectionSummary(selectedCategory, tasks) {
  const openCount = tasks.filter((task) => !isMaintenanceComplete(task)).length;
  const reminderCount = tasks.filter((task) => isMaintenanceReminderActive(task) && !isMaintenanceOverdue(task)).length;
  const overdueCount = tasks.filter((task) => isMaintenanceOverdue(task)).length;
  const installedAssetCount = new Set(tasks.map((task) => String(task.assetId || "")).filter(Boolean)).size;
  const displayCategory = selectedCategory === "all" ? "All systems" : selectedCategory;
  const nextTask = tasks
    .slice()
    .sort((left, right) => getMaintenanceTaskSortValue(left) - getMaintenanceTaskSortValue(right))
    .find((task) => !isMaintenanceComplete(task) || isMaintenanceReminderActive(task));

  return `
    <div>
      <span class="metric-label">Selected section</span>
      <h3 class="card-title maintenance-section-title">${escapeHtml(displayCategory)}</h3>
      <p class="small-copy maintenance-section-copy">
        ${
          selectedCategory === "all"
            ? "Use All systems for a vessel-wide service board, or jump into one installed system for a tighter daily workflow."
            : `Focused service board for ${escapeHtml(displayCategory)} on this vessel.`
        }
      </p>
      <p class="small-copy maintenance-section-copy">
        ${
          nextTask
            ? `Next up: ${escapeHtml(nextTask.title)} • ${escapeHtml(getMaintenanceDueHint(nextTask).detail)}`
            : "Nothing is scheduled inside this view right now."
        }
      </p>
      ${
        isOwnerReadOnly()
          ? '<p class="small-copy maintenance-owner-note">Owner view is read-only. Service status stays visible while edit controls stay hidden.</p>'
          : ""
      }
    </div>
    <div class="maintenance-section-stats">
      <div class="maintenance-section-stat">
        <span class="detail-label">Active</span>
        <strong>${openCount}</strong>
      </div>
      <div class="maintenance-section-stat">
        <span class="detail-label">Due soon</span>
        <strong>${reminderCount}</strong>
      </div>
      <div class="maintenance-section-stat">
        <span class="detail-label">Overdue</span>
        <strong>${overdueCount}</strong>
      </div>
      <div class="maintenance-section-stat">
        <span class="detail-label">Assets</span>
        <strong>${installedAssetCount}</strong>
      </div>
    </div>
  `;
}

function renderMaintenanceTaskGroup(group) {
  const overdueCount = group.tasks.filter((task) => isMaintenanceOverdue(task)).length;
  const dueSoonCount = group.tasks.filter((task) => isMaintenanceDueSoon(task) && !isMaintenanceOverdue(task)).length;
  const completedCount = group.tasks.filter((task) => isMaintenanceComplete(task)).length;

  return `
    <section class="maintenance-task-group">
      <div class="maintenance-task-group-heading">
        <div>
          <p class="eyebrow">System board</p>
          <h3>${escapeHtml(group.category)}</h3>
        </div>
        <div class="maintenance-task-group-stats">
          <span class="maintenance-inline-hint">${group.tasks.length} tasks</span>
          <span class="maintenance-inline-hint is-warn">${dueSoonCount} due soon</span>
          <span class="maintenance-inline-hint ${overdueCount ? "is-danger" : "is-good"}">${overdueCount} overdue</span>
          <span class="maintenance-inline-hint is-good">${completedCount} completed</span>
        </div>
      </div>
      <div class="maintenance-section-board">
        ${group.tasks.map((task) => renderMaintenanceSectionCard(task)).join("")}
      </div>
    </section>
  `;
}

function renderMaintenanceAssets() {
  if (!elements.maintenanceAssetsList) {
    return;
  }

  const ownerMode = isOwnerReadOnly();

  if (!state.maintenanceAssets.length) {
    elements.maintenanceAssetsList.innerHTML = `
      <div class="maintenance-empty-state">
        <span class="metric-label">Quick start</span>
        <h4>Set up your vessel maintenance</h4>
        <p class="small-copy">Harbor Command can automatically generate recurring service from your vessel systems.</p>
        ${
          ownerMode
            ? '<p class="small-copy maintenance-owner-note">This vessel has not been configured yet. A captain or management user can install equipment and starter templates.</p>'
            : `
              <div class="maintenance-empty-actions">
                <button class="primary-button" type="button" data-maintenance-quick-action="starter-pack">Apply Starter Maintenance Pack</button>
                <button class="ghost-button maintenance-ghost-button" type="button" data-maintenance-quick-action="add-equipment">Install Individual Equipment</button>
                <button class="text-button" type="button" data-maintenance-quick-action="start-scratch">Start from scratch</button>
              </div>
            `
        }
      </div>
    `;
    return;
  }

  elements.maintenanceAssetsList.innerHTML = state.maintenanceAssets
    .slice()
    .sort((left, right) => compareTextValues(left.name, right.name))
    .map((asset) => {
      const linkedTasks = state.maintenance.filter((task) => String(task.assetId) === String(asset.id));
      const overdueTasks = linkedTasks.filter((task) => isMaintenanceOverdue(task)).length;
      const reminderTasks = linkedTasks.filter((task) => isMaintenanceReminderActive(task)).length;
      const nextDueTask = linkedTasks
        .slice()
        .sort((left, right) => getMaintenanceTaskSortValue(left) - getMaintenanceTaskSortValue(right))
        .find((task) => !isMaintenanceComplete(task) || isMaintenanceReminderActive(task));
      const template = getMaintenanceTemplateById(asset.templateId);

      return `
        <article class="maintenance-asset-card">
          <div class="maintenance-asset-topline">
            <div>
              <span class="metric-label">${escapeHtml(template?.name || asset.assetType || "Custom asset")}</span>
              <h4 class="card-title">${escapeHtml(asset.name)}</h4>
            </div>
            ${
              ownerMode
                ? ""
                : `
                  <div class="maintenance-asset-actions">
                    <button class="text-button" type="button" data-maintenance-asset-action="edit" data-asset-id="${asset.id}">Edit</button>
                    <button class="text-button danger" type="button" data-maintenance-asset-action="delete" data-asset-id="${asset.id}">Delete</button>
                  </div>
                `
            }
          </div>
          <div class="maintenance-asset-meta">
            <span>${escapeHtml([asset.manufacturer, asset.model].filter(Boolean).join(" ").trim() || "Manual asset")}</span>
            <span>${escapeHtml(asset.location || "No location set")}</span>
            <span>${escapeHtml(asset.meterSourceType === "manual" ? `${asset.currentHours} hrs` : asset.meterSourceType.replace("-", " ") || "No meter")}</span>
          </div>
          <div class="maintenance-asset-stats">
            <div class="detail-item">
              <span class="detail-label">Recurring tasks</span>
              <span class="detail-value">${linkedTasks.length}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Reminders</span>
              <span class="detail-value">${reminderTasks}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Overdue</span>
              <span class="detail-value">${overdueTasks}</span>
            </div>
          </div>
          <p class="small-copy maintenance-asset-copy">
            ${
              nextDueTask
                ? `Next up: ${escapeHtml(nextDueTask.title)} • ${escapeHtml(getMaintenanceDueHint(nextDueTask).detail)}`
                : "No recurring tasks are due right now."
            }
          </p>
        </article>
      `;
    })
    .join("");
}

function renderMaintenanceAssetForm() {
  if (!elements.maintenanceAssetForm) {
    return;
  }

  const fields = elements.maintenanceAssetForm.elements;
  const asset = state.maintenanceAssets.find((item) => item.id === editingMaintenanceAssetId) || null;

  fields.namedItem("templateId").innerHTML = buildMaintenanceTemplateOptions(asset?.templateId || "");

  if (!asset) {
    fields.namedItem("meterSourceId").innerHTML = buildMaintenanceMeterSourceOptions(
      fields.namedItem("meterSourceType").value,
      ""
    );
    elements.maintenanceAssetSubmit.textContent = "Install asset";
    elements.maintenanceAssetCancel.hidden = true;
    renderMaintenanceTemplatePreview(getMaintenanceTemplateById(fields.namedItem("templateId").value));
    return;
  }

  fields.namedItem("templateId").value = asset.templateId || "";
  fields.namedItem("name").value = asset.name || "";
  fields.namedItem("assetType").value = asset.assetType || "";
  fields.namedItem("manufacturer").value = asset.manufacturer || "";
  fields.namedItem("model").value = asset.model || "";
  fields.namedItem("serialNumber").value = asset.serialNumber || "";
  fields.namedItem("location").value = asset.location || "";
  fields.namedItem("meterSourceType").value = asset.meterSourceType || "none";
  fields.namedItem("meterSourceId").innerHTML = buildMaintenanceMeterSourceOptions(asset.meterSourceType, asset.meterSourceId);
  fields.namedItem("meterSourceId").value = asset.meterSourceId || "";
  fields.namedItem("currentHours").value = String(asset.currentHours || 0);
  fields.namedItem("notes").value = asset.notes || "";
  elements.maintenanceAssetSubmit.textContent = "Save asset";
  elements.maintenanceAssetCancel.hidden = false;
  renderMaintenanceTemplatePreview(getMaintenanceTemplateById(asset.templateId));
}

function renderMaintenanceHistory() {
  if (!elements.maintenanceHistoryList) {
    return;
  }

  if (!state.maintenanceHistory.length) {
    elements.maintenanceHistoryList.innerHTML = `
      <div class="empty-state">
        No service completions logged yet. When recurring maintenance is completed here or through linked Work Orders, Harbor Command will write the service history automatically.
      </div>
    `;
    return;
  }

  elements.maintenanceHistoryList.innerHTML = state.maintenanceHistory
    .slice(0, 12)
    .map((entry) => {
      const asset = getMaintenanceAssetById(entry.assetId);
      const metaParts = [];
      if (asset?.name) {
        metaParts.push(asset.name);
      }
      if (entry.completedHours) {
        metaParts.push(`${formatNumberValue(entry.completedHours)} hrs`);
      }
      metaParts.push(entry.source === "work-order" ? "Completed from work order" : "Completed manually");
      if (entry.notes || entry.workDone) {
        metaParts.push(entry.notes || entry.workDone);
      }

      return `
        <article class="maintenance-history-entry">
          <div class="maintenance-history-topline">
            <div>
              <span class="metric-label">Recent service</span>
              <strong>${escapeHtml(`${getMaintenanceHistoryTaskLabel(entry)} completed ${formatOptionalShortDate(entry.completionDate)}`)}</strong>
            </div>
            <span class="maintenance-inline-hint ${entry.source === "work-order" ? "is-good" : ""}">${escapeHtml(formatOptionalShortDate(entry.completionDate))}</span>
          </div>
          <div class="maintenance-history-meta">
            ${metaParts.map((part) => `<span>${escapeHtml(part)}</span>`).join("")}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderMaintenance() {
  const ownerMode = isOwnerReadOnly();
  const allTasks = state.maintenance.slice();
  const categories = getMaintenanceCategoryOptions(allTasks);
  const selectedCategory = getResolvedMaintenanceCategory(categories);
  state.activeMaintenanceCategory = selectedCategory;
  const categoryScopedTasks = selectedCategory === "all"
    ? allTasks
    : allTasks.filter((task) => task.category === selectedCategory);
  const tasks = getFilteredMaintenanceTasks();
  const filterMeta = [
    { id: "all", label: "All tasks", count: categoryScopedTasks.length },
    { id: "due-soon", label: "Due soon", count: categoryScopedTasks.filter((task) => isMaintenanceDueSoon(task)).length },
    { id: "completed", label: "Completed", count: categoryScopedTasks.filter((task) => isMaintenanceComplete(task)).length },
    { id: "overdue", label: "Overdue", count: categoryScopedTasks.filter((task) => isMaintenanceOverdue(task)).length },
  ];
  const reminderTasks = getMaintenanceReminderTasks(allTasks);
  const overdueTasks = allTasks.filter((task) => isMaintenanceOverdue(task));
  const dueTodayTasks = allTasks.filter((task) => isMaintenanceDueToday(task));
  const completedThisWeek = state.maintenanceHistory.filter((entry) => wasDateWithinDays(entry.completionDate, 7));
  const assetBuilderShell = elements.maintenanceAssetForm?.closest(".maintenance-builder-shell");

  renderMaintenanceWorkspacePanels();

  if (elements.maintenanceQuickActions) {
    elements.maintenanceQuickActions.hidden = ownerMode;
  }
  if (assetBuilderShell) {
    assetBuilderShell.hidden = ownerMode;
  }
  if (elements.maintenanceForm) {
    elements.maintenanceForm.hidden = ownerMode;
  }
  if (elements.maintenanceApplyStarterPack) {
    elements.maintenanceApplyStarterPack.hidden = ownerMode || state.maintenanceAssets.length > 0;
  }

  elements.maintenanceFilters.innerHTML = filterMeta
    .map(
      (filter) => `
        <button
          class="maintenance-filter ${state.activeMaintenanceFilter === filter.id ? "active" : ""}"
          type="button"
          data-maintenance-filter="${filter.id}"
        >
          ${filter.label}
          <span>${filter.count}</span>
        </button>
      `
    )
    .join("");

  elements.maintenanceSections.innerHTML = renderMaintenanceSections(categories, allTasks);
  elements.maintenanceSectionSummary.innerHTML = renderMaintenanceSectionSummary(selectedCategory, categoryScopedTasks);
  elements.maintenanceSearch.value = state.activeMaintenanceQuery;
  elements.maintenanceSort.value = state.activeMaintenanceSort;
  elements.maintenanceFocus.innerHTML = renderMaintenanceFocus(reminderTasks, overdueTasks, dueTodayTasks, completedThisWeek, allTasks.length);
  if (elements.maintenanceOverviewSpotlight) {
    elements.maintenanceOverviewSpotlight.innerHTML = renderMaintenanceOverviewSpotlight(
      allTasks,
      reminderTasks,
      overdueTasks,
      dueTodayTasks,
      completedThisWeek
    );
  }

  if (!tasks.length) {
    elements.maintenanceList.innerHTML = `<div class="empty-state">${
      state.activeMaintenanceQuery
        ? `No maintenance tasks match "${escapeHtml(state.activeMaintenanceQuery)}" in this view right now.`
        : "No maintenance tasks match this view right now."
    }</div>`;
    return;
  }

  if (selectedCategory === "all" && state.activeMaintenanceSort === "category") {
    elements.maintenanceList.innerHTML = `
      <div class="maintenance-task-groups">
        ${groupMaintenanceTasks(tasks).map((group) => renderMaintenanceTaskGroup(group)).join("")}
      </div>
    `;
    return;
  }

  elements.maintenanceList.innerHTML = `
    <div class="maintenance-section-board">
      ${tasks.map((task) => renderMaintenanceSectionCard(task)).join("")}
    </div>
  `;
}

function renderMaintenanceTableRow(task, showCategory) {
  const displayStatus = getMaintenanceDisplayStatus(task);
  const dueHint = getMaintenanceDueHint(task);
  const cadenceCopy = formatMaintenanceCadence(task);
  const linkedWorkOrder = getLinkedWorkOrderForMaintenance(task);
  const linkedAsset = getMaintenanceAssetById(task.assetId);

  return `
    <div class="maintenance-row maintenance-body-row">
      <div class="maintenance-cell maintenance-task-cell">
        <strong class="maintenance-item-title">${escapeHtml(task.title)}</strong>
        <div class="maintenance-item-meta">
          <span class="maintenance-inline-hint ${dueHint.toneClass}">${escapeHtml(dueHint.label)}</span>
          <span class="maintenance-inline-copy">${escapeHtml(cadenceCopy)}</span>
          ${
            linkedAsset
              ? `<span class="maintenance-inline-copy">${escapeHtml(linkedAsset.name)}</span>`
              : ""
          }
          ${
            linkedWorkOrder
              ? `<span class="maintenance-inline-hint is-good">${escapeHtml(linkedWorkOrder.originType === "maintenance-suggestion" ? "Suggested in Work Orders" : "Linked in Work Orders")}</span>`
              : ""
          }
        </div>
        <p class="maintenance-item-notes">${escapeHtml(task.notes || "No notes added yet.")}</p>
        <div class="maintenance-table-actions">
          <button class="maintenance-action" type="button" data-task-action="edit" data-task-id="${task.id}">
            Edit
          </button>
          <button class="maintenance-action" type="button" data-task-action="toggle" data-task-id="${task.id}">
            ${isMaintenanceComplete(task) ? "Reopen" : "Complete service"}
          </button>
          ${
            linkedWorkOrder
              ? `<button class="maintenance-action" type="button" data-task-action="open-work-order" data-task-id="${task.id}" data-work-order-id="${linkedWorkOrder.id}">Open work order</button>`
              : ""
          }
          <button class="maintenance-action maintenance-delete-action" type="button" data-task-action="delete" data-task-id="${task.id}">
            Delete
          </button>
        </div>
      </div>
      <div class="maintenance-cell">
        <span class="status-badge ${statusClass(displayStatus)}">${displayStatus}</span>
      </div>
      <div class="maintenance-cell">
        ${
          showCategory
            ? `<span class="maintenance-category-chip ${categoryClass(task.category)}">${escapeHtml(task.category)}</span>`
            : `<span class="maintenance-inline-copy">${escapeHtml(formatMaintenanceCadence(task))}</span>`
        }
      </div>
      <div class="maintenance-cell">
        <span class="priority-badge ${priorityClass(task.priority)}">${task.priority}</span>
      </div>
      <div class="maintenance-cell maintenance-date-cell">
        ${escapeHtml(formatMaintenanceDueValue(task))}
        <span class="maintenance-date-hint ${dueHint.toneClass}">${escapeHtml(dueHint.detail)}</span>
      </div>
      <div class="maintenance-cell maintenance-date-cell">
        ${escapeHtml(formatMaintenanceLastCompletedValue(task))}
        <span class="maintenance-date-hint">${escapeHtml(formatMaintenanceLastCompletedCopy(task))}</span>
      </div>
    </div>
  `;
}

function renderMaintenanceSectionCard(task) {
  const ownerMode = isOwnerReadOnly();
  const displayStatus = getMaintenanceDisplayStatus(task);
  const dueHint = getMaintenanceDueHint(task);
  const linkedWorkOrder = getLinkedWorkOrderForMaintenance(task);
  const linkedAsset = getMaintenanceAssetById(task.assetId);
  const noteCopy = String(task.notes || linkedAsset?.name || "").trim();
  const cadenceCopy = formatMaintenanceCadence(task)
    .replace(" | remind ", " • remind ")
    .replace(/\bor\b/g, "and");

  return `
    <article class="maintenance-section-card">
      <div class="maintenance-section-card-topline">
        <div>
          <h3 class="card-title">${escapeHtml(task.title)}</h3>
          <p class="small-copy maintenance-section-card-copy">${escapeHtml(noteCopy || "No extra notes on this service item yet.")}</p>
        </div>
        <span class="priority-badge ${priorityClass(task.priority)}">${task.priority}</span>
      </div>
      <div class="maintenance-section-card-meta">
        <span class="status-badge ${statusClass(displayStatus)}">${displayStatus}</span>
        <span class="maintenance-inline-hint ${dueHint.toneClass}">${escapeHtml(dueHint.label)}</span>
        <span class="maintenance-category-chip ${categoryClass(task.category)}">${escapeHtml(task.category)}</span>
        ${
          linkedAsset
            ? `<span class="maintenance-inline-hint">${escapeHtml(linkedAsset.name)}</span>`
            : ""
        }
        ${
          linkedWorkOrder
            ? `<span class="maintenance-inline-hint is-good">${escapeHtml(linkedWorkOrder.originType === "maintenance-suggestion" ? "Suggested in Work Orders" : "Linked in Work Orders")}</span>`
            : ""
        }
      </div>
      <div class="maintenance-section-card-grid">
        <div class="detail-item">
          <span class="detail-label">Next due</span>
          <span class="detail-value">${escapeHtml(formatMaintenanceDueValue(task, true))}</span>
          <span class="maintenance-section-detail-copy ${dueHint.toneClass}">${escapeHtml(dueHint.detail)}</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Last completed</span>
          <span class="detail-value">${escapeHtml(formatMaintenanceLastCompletedValue(task, true))}</span>
          <span class="maintenance-section-detail-copy">${escapeHtml(formatMaintenanceLastCompletedCopy(task))}</span>
        </div>
        <div class="detail-item">
          <span class="detail-label">Cadence</span>
          <span class="detail-value">${escapeHtml(cadenceCopy)}</span>
          <span class="maintenance-section-detail-copy">${escapeHtml(getMaintenanceMeterSummary(task))}</span>
        </div>
      </div>
      ${
        ownerMode
          ? ""
          : `
            <div class="maintenance-section-card-actions">
              ${
                isMaintenanceComplete(task)
                  ? `<button class="text-button" type="button" data-task-action="toggle" data-task-id="${task.id}">Reopen</button>`
                  : `<button class="primary-button maintenance-primary-action" type="button" data-task-action="toggle" data-task-id="${task.id}">Complete service</button>`
              }
              <button class="text-button" type="button" data-task-action="edit" data-task-id="${task.id}">Edit</button>
              ${
                linkedWorkOrder
                  ? `<button class="text-button" type="button" data-task-action="open-work-order" data-task-id="${task.id}" data-work-order-id="${linkedWorkOrder.id}">Open work order</button>`
                  : ""
              }
              <button class="text-button danger maintenance-delete-button" type="button" data-task-action="delete" data-task-id="${task.id}">Delete</button>
            </div>
          `
      }
    </article>
  `;
}

function renderMaintenanceForm() {
  const fields = elements.maintenanceForm.elements;
  const task = state.maintenance.find((item) => item.id === editingMaintenanceId) || null;
  fields.namedItem("assetId").innerHTML = buildMaintenanceAssetOptions(task?.assetId || "");

  if (!task) {
    if (editingMaintenanceId) {
      editingMaintenanceId = null;
    }
    const preferredCategory = getResolvedMaintenanceCategory(getMaintenanceCategoryOptions(state.maintenance));
    if (preferredCategory !== "all") {
      fields.namedItem("category").value = preferredCategory;
    }
    if (!fields.namedItem("status").value) {
      fields.namedItem("status").value = "Not Started";
    }
    if (!fields.namedItem("priority").value) {
      fields.namedItem("priority").value = "High";
    }
    fields.namedItem("recurrenceMode").value = "days";
    fields.namedItem("dueHours").value = "0";
    fields.namedItem("lastCompletedHours").value = "0";
    fields.namedItem("intervalDays").value = String(DEFAULT_MAINTENANCE_INTERVAL_DAYS);
    fields.namedItem("intervalHours").value = String(DEFAULT_MAINTENANCE_INTERVAL_HOURS);
    fields.namedItem("reminderDays").value = String(DEFAULT_MAINTENANCE_REMINDER_DAYS);
    fields.namedItem("reminderHours").value = String(DEFAULT_MAINTENANCE_REMINDER_HOURS);
    fields.namedItem("meterSourceType").value = "none";
    fields.namedItem("meterSourceId").innerHTML = buildMaintenanceMeterSourceOptions("none", "");
    fields.namedItem("notes").value = "";
    if (elements.maintenanceAdvanced) {
      elements.maintenanceAdvanced.open = false;
    }
    elements.maintenanceSubmit.textContent = "Add task";
    elements.maintenanceCancel.hidden = true;
    return;
  }

  fields.namedItem("title").value = task.title;
  fields.namedItem("category").value = task.category;
  fields.namedItem("dueDate").value = task.dueDate;
  fields.namedItem("dueHours").value = String(task.dueHours || 0);
  fields.namedItem("status").value = task.status;
  fields.namedItem("priority").value = task.priority;
  fields.namedItem("lastCompleted").value = task.lastCompleted;
  fields.namedItem("lastCompletedHours").value = String(task.lastCompletedHours || 0);
  fields.namedItem("assetId").value = task.assetId || "";
  fields.namedItem("recurrenceMode").value = task.recurrenceMode || "days";
  fields.namedItem("intervalDays").value = String(getMaintenanceIntervalDays(task));
  fields.namedItem("intervalHours").value = String(getMaintenanceIntervalHours(task));
  fields.namedItem("reminderDays").value = String(getMaintenanceReminderDays(task));
  fields.namedItem("reminderHours").value = String(getMaintenanceReminderHours(task));
  fields.namedItem("meterSourceType").value = task.meterSourceType || "none";
  fields.namedItem("meterSourceId").innerHTML = buildMaintenanceMeterSourceOptions(task.meterSourceType, task.meterSourceId);
  fields.namedItem("meterSourceId").value = task.meterSourceId || "";
  fields.namedItem("notes").value = task.notes;
  if (elements.maintenanceAdvanced) {
    elements.maintenanceAdvanced.open = Boolean(
      (task.meterSourceType && task.meterSourceType !== "none")
      || task.meterSourceId
    );
  }
  elements.maintenanceSubmit.textContent = "Save changes";
  elements.maintenanceCancel.hidden = false;
}

function buildWorkOrderMaintenanceOptions(selectedId = "") {
  const selectedValue = String(selectedId || "").trim();
  const options = [
    `<option value="">No linked maintenance task</option>`,
  ];
  const tasks = getSortedMaintenanceTasks(state.maintenance);

  tasks.forEach((task) => {
    const linkedOrder = state.workOrders.find((order) =>
      String(order.maintenanceLogId) === String(task.id) && !isWorkOrderComplete(order)
    );
    const linkedLabel = linkedOrder
      ? linkedOrder.originType === "maintenance-suggestion"
        ? "Suggested this week"
        : "Linked in Work Orders"
      : "";
    options.push(`
      <option value="${escapeHtml(task.id)}" ${selectedValue === String(task.id) ? "selected" : ""}>
        ${escapeHtml(task.title)}${linkedLabel ? ` - ${escapeHtml(linkedLabel)}` : ""}
      </option>
    `);
  });

  return options.join("");
}

function getWorkOrderRelationshipCopy(order) {
  const linkedTask = getLinkedMaintenanceTask(order);
  if (!linkedTask) {
    return "";
  }

  if (order.originType === "maintenance-suggestion") {
    return `Suggested from ${linkedTask.category} maintenance`;
  }

  return `Linked to ${linkedTask.category} maintenance`;
}

function renderWorkOrders() {
  const currentWeek = getActiveWorkWeekRange();
  const weekEntries = getCurrentWeekWorkOrders();
  const linkedReport = state.reports.find(
    (report) => report.weekStart === currentWeek.start && report.weekEnd === currentWeek.end
  ) || null;
  const systemsCount = weekEntries.filter((entry) => String(entry.systemsChecked || "").trim()).length;
  const issueCount = weekEntries.filter((entry) => String(entry.issues || "").trim()).length;
  const noteCount = weekEntries.filter((entry) => String(entry.notes || "").trim()).length;
  const completedCount = weekEntries.filter((entry) => isWorkOrderComplete(entry)).length;
  const linkedMaintenanceCount = weekEntries.filter((entry) => String(entry.maintenanceLogId || "").trim()).length;
  const suggestedCount = weekEntries.filter((entry) => String(entry.originType || "").trim() === "maintenance-suggestion").length;
  const reportStatusLabel = linkedReport
    ? linkedReport.status === "finalized" ? "Finalized" : "Draft"
    : "Not generated";

  elements.workOrderTitle.textContent = `${state.vessel.name} weekly workspace`;
  elements.workOrderPeriod.textContent = formatReportPeriod(currentWeek);
  elements.workOrderSort.value = state.activeWorkOrderSort;
  elements.workOrderListCopy.textContent = weekEntries.length
    ? `Live Monday through Friday workspace for ${formatDate(currentWeek.start)} through ${formatDate(currentWeek.end)}. Generate a report when this saved week is ready for handoff.`
    : `No weekly entries logged yet for ${formatDate(currentWeek.start)} through ${formatDate(currentWeek.end)}. Start the workspace here, then generate the saved report from these entries.`;
  elements.workOrderActionNote.textContent = linkedReport
    ? linkedReport.status === "finalized"
      ? "This week already has a finalized report in the library. Reopen it in Reports before generating a fresh snapshot from the live workspace."
      : "Generating again will refresh the draft report snapshot in Reports using the saved entries below."
    : linkedMaintenanceCount || suggestedCount
      ? suggestedCount
        ? "This is the live weekly workspace for the active vessel. Suggested maintenance entries are already staged here, and linked items can update recurring service automatically when you mark them complete."
        : "This is the live weekly workspace for the active vessel. Linked maintenance items can complete recurring service automatically when you mark them done here."
      : "This is the live weekly workspace for the active vessel. Add, edit, and delete entries here during the week, then generate the saved report from this workspace.";

  const summaryCards = [
    {
      label: "Entries",
      value: `${weekEntries.length}`,
      copy: "Saved rows in the current work week.",
    },
    {
      label: "Systems checked",
      value: `${systemsCount}`,
      copy: "Entries with inspections or system checks logged.",
    },
    {
      label: "Issues",
      value: `${issueCount}`,
      copy: issueCount ? "Entries carrying open findings this week." : "No issues logged in this workspace yet.",
    },
    {
      label: "Completed",
      value: `${completedCount}`,
      copy: linkedMaintenanceCount
        ? suggestedCount
          ? `${linkedMaintenanceCount} linked entries, including ${suggestedCount} maintenance suggestions.`
          : `${linkedMaintenanceCount} entries linked to maintenance automation.`
        : linkedReport
          ? `Saved report for ${formatMonthDay(currentWeek.start)} - ${formatMonthDay(currentWeek.end)}.`
          : "Generate the weekly report when ready.",
    },
  ];

  elements.workOrderSummaryCards.innerHTML = summaryCards
    .map(
      (card) => `
        <article class="report-summary-card">
          <span class="metric-label">${card.label}</span>
          <strong class="report-summary-value">${escapeHtml(card.value)}</strong>
          <p class="small-copy">${card.copy}</p>
        </article>
      `
    )
    .join("");

  if (!weekEntries.length) {
    elements.workOrderList.innerHTML = `<div class="empty-state">No weekly entries have been logged for ${state.vessel.name} yet. Start this week's workspace here.</div>`;
    return;
  }

  elements.workOrderList.innerHTML = `
    <div class="weekly-log-board">
      <div class="weekly-log-row weekly-log-header">
        <div class="weekly-log-cell weekly-log-item">Area / task</div>
        <div class="weekly-log-cell">Date</div>
        <div class="weekly-log-cell">Work done</div>
        <div class="weekly-log-cell">Systems checked</div>
        <div class="weekly-log-cell">Issues</div>
        <div class="weekly-log-cell">Notes</div>
      </div>
      ${weekEntries
        .map(
          (entry) => `
            <div class="weekly-log-row weekly-log-body-row">
              <div class="weekly-log-cell weekly-log-item">
                <strong>${escapeHtml(entry.item)}</strong>
                ${
                  entry.maintenanceLogId
                    ? `<div class="work-order-meta-row"><span class="work-order-link-chip">${escapeHtml(getWorkOrderRelationshipCopy(entry) || "Linked maintenance")}</span></div>`
                    : ""
                }
                <div class="weekly-log-actions">
                  <button class="text-button report-action-button" type="button" data-work-order-action="edit" data-work-order-id="${entry.id}">
                    Edit
                  </button>
                  <button class="text-button report-action-button" type="button" data-work-order-action="toggle" data-work-order-id="${entry.id}">
                    ${isWorkOrderComplete(entry) ? "Reopen" : "Complete"}
                  </button>
                  <button class="text-button report-action-button danger" type="button" data-work-order-action="delete" data-work-order-id="${entry.id}">
                    Delete
                  </button>
                </div>
              </div>
              <div class="weekly-log-cell">
                <span class="status-badge ${statusClass(isWorkOrderComplete(entry) ? "Completed" : "Open")}">${isWorkOrderComplete(entry) ? "Completed" : "Open"}</span>
                <div class="work-order-date-copy">${formatDate(entry.reportDate)}</div>
              </div>
              <div class="weekly-log-cell">${renderLogValue(entry.workDone)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.systemsChecked)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.issues)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.notes)}</div>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function renderWorkOrderForm() {
  const fields = elements.workOrderForm.elements;
  const order = state.workOrders.find((item) => item.id === editingWorkOrderId) || null;
  const currentWeek = getActiveWorkWeekRange();

  elements.workOrderFormContext.textContent = `Entries in this workspace stay inside ${formatDate(currentWeek.start)} through ${formatDate(currentWeek.end)} for ${state.vessel.name}.`;
  fields.namedItem("maintenanceLogId").innerHTML = buildWorkOrderMaintenanceOptions(order?.maintenanceLogId || "");

  if (!order) {
    if (editingWorkOrderId) {
      editingWorkOrderId = null;
    }
    fields.namedItem("reportDate").value = getPreferredCurrentWorkOrderDate();
    fields.namedItem("maintenanceLogId").value = "";
    elements.workOrderSubmit.textContent = "Add entry";
    elements.workOrderCancel.hidden = true;
    return;
  }

  fields.namedItem("item").value = order.item;
  fields.namedItem("reportDate").value = order.reportDate;
  fields.namedItem("maintenanceLogId").value = order.maintenanceLogId || "";
  fields.namedItem("workDone").value = order.workDone || "";
  fields.namedItem("systemsChecked").value = order.systemsChecked || "";
  fields.namedItem("issues").value = order.issues || "";
  fields.namedItem("notes").value = order.notes;
  elements.workOrderSubmit.textContent = "Save changes";
  elements.workOrderCancel.hidden = false;
}

function renderCharters() {
  const charters = getSortedCharters();
  elements.charterSort.value = state.activeCharterSort;

  if (!charters.length) {
    elements.charterList.innerHTML = `<div class="empty-state">No charters planned for ${state.vessel.name} yet.</div>`;
    return;
  }

  elements.charterList.innerHTML = charters
    .map(
      (charter) => `
        <article class="charter-card">
          <div class="card-topline">
            <div>
              <h3 class="card-title">${charter.client}</h3>
              <p class="card-meta">${formatDate(charter.start)} to ${formatDate(charter.end)}</p>
            </div>
            <span class="status-badge ${statusClass(charter.status)}">${charter.status}</span>
          </div>
          <p class="small-copy">Destination berth: ${charter.berth}</p>
        </article>
      `
    )
    .join("");
}

function renderCrew() {
  if (!state.crew.length) {
    elements.crewList.innerHTML = `<div class="empty-state">No crew assigned to ${state.vessel.name} yet.</div>`;
    return;
  }

  elements.crewList.innerHTML = state.crew
    .map(
      (member) => `
        <article class="crew-card">
          <div class="crew-topline">
            <div>
              <h3 class="crew-name">${member.name}</h3>
              <p class="crew-meta">${member.role} | ${member.rotation}</p>
            </div>
            <span class="status-badge status-scheduled">Assigned</span>
          </div>
          <div class="crew-details">
            <p class="small-copy">Certification: ${member.certification}</p>
            <p class="small-copy">Rotation: ${member.rotation}</p>
            <p class="small-copy">Assigned vessel: ${state.vessel.name}</p>
          </div>
        </article>
      `
    )
    .join("");
}

function renderAccessModal() {
  if (!elements.usersList || !elements.usersFeedback || !elements.inviteList || !elements.inviteFeedback) {
    return;
  }

  const allowed = canManageUsers();
  const manageableVessels = getManageableVesselsForAccessModal();
  const selectedInviteRole = String(elements.inviteForm?.elements.namedItem("role")?.value || "Crew");
  const inviteHasFullFleetAccess = roleHasFullFleetAccess(selectedInviteRole);
  const inviteSelectedVesselIds = readCheckedVesselIds(elements.inviteVesselPicker);
  elements.inviteForm.hidden = !allowed;
  elements.inviteSubmit.disabled = managedInvitesState.loading;
  elements.inviteSubmit.textContent = managedInvitesState.loading ? "Working..." : "Create invite";

  if (managedInvitesState.error) {
    elements.inviteFeedback.hidden = false;
    elements.inviteFeedback.className = "users-feedback error";
    elements.inviteFeedback.textContent = managedInvitesState.error;
  } else if (managedInvitesState.notice) {
    elements.inviteFeedback.hidden = false;
    elements.inviteFeedback.className = "users-feedback success";
    elements.inviteFeedback.textContent = managedInvitesState.notice;
  } else {
    elements.inviteFeedback.hidden = true;
    elements.inviteFeedback.className = "users-feedback";
    elements.inviteFeedback.textContent = "";
  }

  if (managedUsersState.error) {
    elements.usersFeedback.hidden = false;
    elements.usersFeedback.className = "users-feedback error";
    elements.usersFeedback.textContent = managedUsersState.error;
  } else if (managedUsersState.notice) {
    elements.usersFeedback.hidden = false;
    elements.usersFeedback.className = "users-feedback success";
    elements.usersFeedback.textContent = managedUsersState.notice;
  } else {
    elements.usersFeedback.hidden = true;
    elements.usersFeedback.className = "users-feedback";
    elements.usersFeedback.textContent = "";
  }

  if (!allowed) {
    elements.invitePreview.innerHTML = "";
    if (elements.inviteVesselPicker) {
      elements.inviteVesselPicker.innerHTML = "";
    }
    if (elements.inviteVesselHelp) {
      elements.inviteVesselHelp.textContent = "";
    }
    elements.inviteList.innerHTML = `<div class="empty-state">Only Captain or Management can create and manage invites.</div>`;
    elements.usersList.innerHTML = `<div class="empty-state">Only Captain or Management can manage Harbor Command users.</div>`;
    return;
  }

  if (elements.inviteVesselPicker) {
    elements.inviteVesselPicker.innerHTML = inviteHasFullFleetAccess
      ? `<div class="access-vessel-note">Captain and Management accounts receive full fleet access automatically.</div>`
      : renderVesselAccessCheckboxes(manageableVessels, inviteSelectedVesselIds, { inputName: "invite-vessel-access" });
  }
  if (elements.inviteVesselHelp) {
    elements.inviteVesselHelp.textContent = inviteHasFullFleetAccess
      ? "This role can access every vessel in Harbor Command."
      : manageableVessels.length
        ? "Choose which vessels this user can open and manage."
        : "Add a vessel first, then assign access here.";
  }

  const delivery = normalizeInviteDelivery(managedInvitesState.delivery);
  const deliveryMarkup = `
    <article class="invite-preview-card invite-delivery-card ${delivery.ready ? "is-ready" : "is-pending"}">
      <p class="small-copy">Email delivery</p>
      <strong>${delivery.ready ? "Resend is ready" : "Copy-link fallback"}</strong>
      <p class="small-copy">${escapeHtml(delivery.message || "Invite links can still be copied and shared manually.")}</p>
      ${
        delivery.ready
          ? `<p class="small-copy">From: ${escapeHtml(delivery.fromEmail)} | App URL: ${escapeHtml(delivery.publicAppUrl)}</p>`
          : ""
      }
    </article>
  `;

  if (managedInvitesState.latestInvite) {
    const latestInviteLink = managedInvitesState.latestInvite.link || buildInviteLink(managedInvitesState.latestInvite.token);
    const accessSummary = managedInvitesState.latestInvite.hasFullFleetAccess
      ? "Full fleet access"
      : describeVesselAccess(managedInvitesState.latestInvite.vesselIds, manageableVessels);
    elements.invitePreview.innerHTML = `
      ${deliveryMarkup}
      <article class="invite-preview-card">
        <p class="small-copy">Latest invite</p>
        <strong>${escapeHtml(managedInvitesState.latestInvite.email)}</strong>
        <p class="small-copy">Role: ${escapeHtml(managedInvitesState.latestInvite.role)} | ${escapeHtml(accessSummary)} | Expires ${formatDateTime(managedInvitesState.latestInvite.expiresAt)}</p>
        <div class="invite-link-field">${escapeHtml(latestInviteLink)}</div>
        <div class="form-actions">
          <button class="text-button" type="button" data-invite-action="copy" data-invite-id="${managedInvitesState.latestInvite.id}" data-invite-token="${managedInvitesState.latestInvite.token}" data-invite-email="${escapeHtml(managedInvitesState.latestInvite.email)}" data-invite-role="${escapeHtml(managedInvitesState.latestInvite.role)}">Copy link</button>
          <button class="text-button" type="button" data-invite-action="email" data-invite-id="${managedInvitesState.latestInvite.id}" data-invite-token="${managedInvitesState.latestInvite.token}" data-invite-email="${escapeHtml(managedInvitesState.latestInvite.email)}" data-invite-role="${escapeHtml(managedInvitesState.latestInvite.role)}">Send email</button>
        </div>
      </article>
    `;
  } else {
    elements.invitePreview.innerHTML = deliveryMarkup;
  }

  if (managedInvitesState.loading && !managedInvitesState.items.length) {
    elements.inviteList.innerHTML = `<div class="empty-state">Loading pending invites...</div>`;
  } else if (!managedInvitesState.items.length) {
    elements.inviteList.innerHTML = `<div class="empty-state">No pending invites right now.</div>`;
  } else {
    elements.inviteList.innerHTML = managedInvitesState.items
      .map((invite) => {
        const link = invite.link || buildInviteLink(invite.token);
        const accessSummary = invite.hasFullFleetAccess
          ? "Full fleet access"
          : describeVesselAccess(invite.vesselIds, manageableVessels);
        return `
          <article class="invite-card">
            <div class="card-topline">
              <div>
                <h3 class="card-title">${escapeHtml(invite.email)}</h3>
                <p class="card-meta">${escapeHtml(invite.role)} | ${escapeHtml(accessSummary)} | Expires ${formatDateTime(invite.expiresAt)}</p>
              </div>
              <span class="status-badge status-active">Pending</span>
            </div>
            <div class="invite-link-field">${escapeHtml(link)}</div>
            <div class="row-actions">
              <button class="text-button" type="button" data-invite-action="copy" data-invite-id="${invite.id}" data-invite-token="${invite.token}" data-invite-email="${escapeHtml(invite.email)}" data-invite-role="${escapeHtml(invite.role)}">Copy link</button>
              <button class="text-button" type="button" data-invite-action="email" data-invite-id="${invite.id}" data-invite-token="${invite.token}" data-invite-email="${escapeHtml(invite.email)}" data-invite-role="${escapeHtml(invite.role)}">Send email</button>
              <button class="text-button danger" type="button" data-invite-action="revoke" data-invite-id="${invite.id}" data-invite-token="${invite.token}" data-invite-email="${escapeHtml(invite.email)}" data-invite-role="${escapeHtml(invite.role)}">Revoke</button>
            </div>
          </article>
        `;
      })
      .join("");
  }

  if (managedUsersState.loading && !managedUsersState.items.length) {
    elements.usersList.innerHTML = `<div class="empty-state">Loading vessel users...</div>`;
    return;
  }

  if (!managedUsersState.items.length) {
    elements.usersList.innerHTML = `<div class="empty-state">No additional users have been added yet.</div>`;
    return;
  }

  elements.usersList.innerHTML = `
    <div class="account-list">
      ${managedUsersState.items
        .map((user) => {
          const isCurrentUser = Number(user.id) === Number(authState.user?.id);
          const accessSummary = user.hasFullFleetAccess
            ? "Full fleet access"
            : describeVesselAccess(user.vesselIds, manageableVessels);
          const accessEditorMarkup = user.hasFullFleetAccess
            ? `<div class="account-access-note"><p class="small-copy">Full fleet access is granted automatically by the ${escapeHtml(user.role)} role.</p></div>`
            : `
              <div class="account-access-editor">
                <p class="small-copy">Vessel access</p>
                ${renderVesselAccessCheckboxes(manageableVessels, user.vesselIds, { inputName: `user-vessel-access-${user.id}`, allowEmpty: true })}
              </div>
            `;
          const actionButtons = isCurrentUser
            ? ""
            : `
              <div class="form-actions account-actions">
                ${user.hasFullFleetAccess ? "" : `<button class="text-button user-table-button" type="button" data-user-action="access" data-user-id="${user.id}" data-user-name="${escapeHtml(user.name)}">Save access</button>`}
                <button class="text-button user-table-button" type="button" data-user-action="toggle" data-user-id="${user.id}" data-user-active="${user.isActive}" data-user-name="${escapeHtml(user.name)}">
                  ${user.isActive ? "Deactivate" : "Activate"}
                </button>
                <button class="text-button user-table-button user-delete-button" type="button" data-user-action="delete" data-user-id="${user.id}" data-user-name="${escapeHtml(user.name)}">
                  Delete
                </button>
              </div>
            `;
          return `
            <article class="account-card">
              <div class="card-topline">
                <div>
                  <h3 class="card-title account-title">${escapeHtml(user.name)}</h3>
                  <p class="account-email">${escapeHtml(user.email)}</p>
                </div>
                <span class="status-badge ${statusClass(user.isActive ? "Active" : "Inactive")}">${user.isActive ? "Active" : "Inactive"}</span>
              </div>
              <div class="account-meta">
                <span class="category-chip ${categoryClass(user.role)}">${escapeHtml(user.role)}</span>
                <span class="account-meta-text">Added ${formatDateTime(user.createdAt)}</span>
                <span class="account-meta-text">${escapeHtml(accessSummary)}</span>
                ${isCurrentUser ? '<span class="account-current-note">Signed in now</span>' : ""}
              </div>
              ${accessEditorMarkup}
              ${actionButtons}
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderReports() {
  ensureActiveReportSelection();
  const selectedReport = getSelectedWeeklyReport();
  const historyOptions = state.reports
    .slice()
    .sort((left, right) => right.weekStart.localeCompare(left.weekStart));
  const reportEntries = selectedReport
    ? selectedReport.entries.slice().sort((left, right) => parseDateValue(left.reportDate) - parseDateValue(right.reportDate))
    : [];
  const systemsCount = reportEntries.filter((entry) => String(entry.systemsChecked || "").trim()).length;
  const issueCount = reportEntries.filter((entry) => String(entry.issues || "").trim()).length;
  const noteCount = reportEntries.filter((entry) => String(entry.notes || "").trim()).length;
  const statusLabel = selectedReport ? (selectedReport.status === "finalized" ? "Finalized" : "Draft") : "No report selected";

  elements.reportHistorySelect.innerHTML = historyOptions.length
    ? historyOptions
        .map(
          (report) => `
            <option value="${escapeHtml(report.id)}" ${selectedReport?.id === report.id ? "selected" : ""}>
              Weekly Log: ${formatMonthDay(report.weekStart)} - ${formatMonthDay(report.weekEnd)}
            </option>
          `
        )
        .join("")
    : `<option value="">No weekly reports yet</option>`;
  elements.reportHistorySelect.disabled = !historyOptions.length;

  elements.reportTitle.textContent = selectedReport
    ? `${state.vessel.name} weekly report`
    : `${state.vessel.name} report library`;
  elements.reportPeriod.textContent = selectedReport
    ? formatReportPeriod(selectedReport)
    : "NO GENERATED REPORTS";
  elements.reportStatusBadge.textContent = statusLabel;
  elements.reportStatusBadge.className = `status-badge ${selectedReport ? statusClass(selectedReport.status) : statusClass("Open")}`;
  elements.reportViewButton.hidden = !selectedReport;
  elements.reportViewButton.disabled = !selectedReport;
  elements.reportViewButton.title = selectedReport
    ? `Jump to the saved preview for ${formatDate(selectedReport.weekStart)} through ${formatDate(selectedReport.weekEnd)}.`
    : "Select a saved weekly report first.";
  elements.reportExportPdf.hidden = !selectedReport;
  elements.reportExportPdf.title = selectedReport
    ? `Open ${state.vessel.name}'s saved weekly report as a PDF.`
    : "Select a saved weekly report to export it as a PDF.";
  elements.reportToggleStatus.hidden = !selectedReport;
  elements.reportToggleStatus.textContent = selectedReport?.status === "finalized" ? "Reopen report" : "Finalize report";
  elements.reportToggleStatus.title = selectedReport?.status === "finalized"
    ? "Reopen this weekly report if you need to regenerate a new snapshot from Work Orders."
    : "Finalize this weekly report to lock the saved snapshot and keep it owner-ready.";
    elements.reportActionNote.textContent = selectedReport
      ? selectedReport.status === "finalized"
        ? `Showing ${state.vessel.name}'s saved report history only. This report is locked and ready for owner review or PDF export.`
        : `Showing ${state.vessel.name}'s saved report history only. Finalize this report when the snapshot is ready to be treated as finished history.`
      : `Showing ${state.vessel.name}'s saved report history only. Generate weekly snapshots from Work Orders and they will appear here.`;
  elements.reportPreviewCopy.textContent = selectedReport
    ? `Previewing the saved snapshot for ${formatDate(selectedReport.weekStart)} through ${formatDate(selectedReport.weekEnd)}.`
    : `Select a saved report from history to preview the entries.`;

  const summaryCards = [
    {
      label: "Reports saved",
      value: `${historyOptions.length}`,
      copy: "Generated weekly reports in this vessel library.",
    },
    {
      label: "Selected entries",
      value: `${reportEntries.length}`,
      copy: "Rows saved in the selected weekly report.",
    },
    {
      label: "Issues",
      value: `${issueCount}`,
      copy: issueCount ? "Saved findings captured in the selected report." : "No issues logged in this report.",
    },
    {
      label: "Generated",
      value: selectedReport ? formatDate(selectedReport.updatedAt || selectedReport.createdAt) : "--",
      copy: selectedReport ? `Status: ${statusLabel}. Systems logged: ${systemsCount}. Notes logged: ${noteCount}.` : "Choose a saved report to view the generated date.",
    },
  ];

  elements.reportSummaryCards.innerHTML = summaryCards
    .map(
      (card) => `
        <article class="report-summary-card">
          <span class="metric-label">${card.label}</span>
          <strong class="report-summary-value">${card.value}</strong>
          <p class="small-copy">${card.copy}</p>
        </article>
      `
    )
    .join("");

  if (!historyOptions.length) {
    elements.reportPreviewList.innerHTML = `<div class="empty-state">Select a generated report to preview it here.</div>`;
    return;
  }

  if (!selectedReport) {
    elements.reportPreviewList.innerHTML = `<div class="empty-state">Select a saved report from history to preview its entries.</div>`;
    return;
  }

  if (!reportEntries.length) {
    elements.reportPreviewList.innerHTML = `<div class="empty-state">No entries were saved for ${formatDate(selectedReport.weekStart)} through ${formatDate(selectedReport.weekEnd)}.</div>`;
    return;
  }

  elements.reportPreviewList.innerHTML = `
    <div class="weekly-log-board">
      <div class="weekly-log-row weekly-log-header">
        <div class="weekly-log-cell weekly-log-item">Area / task</div>
        <div class="weekly-log-cell">Date</div>
        <div class="weekly-log-cell">Work Done</div>
        <div class="weekly-log-cell">Systems Checked</div>
        <div class="weekly-log-cell">Issues</div>
        <div class="weekly-log-cell">Notes</div>
      </div>
      ${reportEntries
        .map(
          (entry) => `
            <div class="weekly-log-row weekly-log-body-row">
              <div class="weekly-log-cell weekly-log-item">
                <strong>${escapeHtml(entry.item)}</strong>
                <p class="small-copy">Saved snapshot row</p>
              </div>
              <div class="weekly-log-cell">${formatDate(entry.reportDate)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.workDone)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.systemsChecked)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.issues)}</div>
              <div class="weekly-log-cell">${renderLogValue(entry.notes)}</div>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function renderVendors() {
  const vendors = getSortedVendors(getFilteredVendors());
  elements.vendorSort.value = state.activeVendorSort;
  const filterMeta = [
    { id: "all", label: "All vendors", count: state.vendors.length },
    {
      id: "active",
      label: "Active vendors",
      count: state.vendors.filter((vendor) => vendor.status === "Active").length,
    },
    {
      id: "under-review",
      label: "Under review",
      count: state.vendors.filter((vendor) => vendor.status === "Under review").length,
    },
  ];

  elements.vendorFilters.innerHTML = filterMeta
    .map(
      (filter) => `
        <button
          class="vendor-filter ${state.activeVendorFilter === filter.id ? "active" : ""}"
          type="button"
          data-vendor-filter="${filter.id}"
        >
          ${filter.label}
          <span>${filter.count}</span>
        </button>
      `
    )
    .join("");

  if (!vendors.length) {
    elements.vendorTable.innerHTML = `<div class="empty-state">No vendors match this view right now.</div>`;
    return;
  }

  elements.vendorTable.innerHTML = `
    <div class="vendor-board">
      <div class="vendor-row vendor-header">
        <div class="vendor-cell vendor-name-cell">Vendor name</div>
        <div class="vendor-cell">Contact person</div>
        <div class="vendor-cell">Email</div>
        <div class="vendor-cell">Phone</div>
        <div class="vendor-cell">Status</div>
        <div class="vendor-cell">Category</div>
        <div class="vendor-cell vendor-actions-header">Actions</div>
      </div>
      ${vendors
        .map(
          (vendor) => `
            <div class="vendor-row vendor-body-row">
              <div class="vendor-cell vendor-name-cell">
                <strong>${escapeHtml(vendor.name)}</strong>
              </div>
              <div class="vendor-cell">${escapeHtml(vendor.contact)}</div>
              <div class="vendor-cell">${escapeHtml(vendor.email)}</div>
              <div class="vendor-cell">${escapeHtml(vendor.phone)}</div>
              <div class="vendor-cell">
                <span class="status-badge ${statusClass(vendor.status)}">${vendor.status}</span>
              </div>
              <div class="vendor-cell">
                <span class="category-chip ${categoryClass(vendor.category)}">${escapeHtml(vendor.category)}</span>
              </div>
              <div class="vendor-cell vendor-actions-cell">
                <button class="text-button vendor-table-button" type="button" data-vendor-action="edit" data-vendor-id="${vendor.id}">
                  Edit
                </button>
                <button class="text-button vendor-table-button vendor-delete-button" type="button" data-vendor-action="delete" data-vendor-id="${vendor.id}">
                  Delete
                </button>
              </div>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function renderVendorForm() {
  const fields = elements.vendorForm.elements;
  const vendor = state.vendors.find((item) => item.id === editingVendorId) || null;

  if (!vendor) {
    if (editingVendorId) {
      editingVendorId = null;
    }
    elements.vendorSubmit.textContent = "Add vendor";
    elements.vendorCancel.hidden = true;
    return;
  }

  fields.namedItem("name").value = vendor.name;
  fields.namedItem("contact").value = vendor.contact;
  fields.namedItem("email").value = vendor.email === "N/A" ? "" : vendor.email;
  fields.namedItem("phone").value = vendor.phone;
  fields.namedItem("status").value = vendor.status;
  fields.namedItem("category").value = vendor.category;
  elements.vendorSubmit.textContent = "Save changes";
  elements.vendorCancel.hidden = false;
}

function renderInventory() {
  const items = getSortedInventoryItems();
  elements.inventorySort.value = state.activeInventorySort;

  if (!items.length) {
    elements.inventoryList.innerHTML = `<div class="empty-state">No inventory items have been logged for ${state.vessel.name} yet.</div>`;
    return;
  }

  elements.inventoryList.innerHTML = items
    .map((item) => {
      const lowStock = Number(item.quantity || 0) <= Number(item.minimumQuantity || 0);
      const itemStatus = lowStock && item.status === "In Stock" ? "Low Stock" : item.status;
      return `
        <article class="maintenance-card inventory-card">
          <div class="card-topline">
            <div>
              <h3 class="card-title">${escapeHtml(item.name)}</h3>
              <p class="card-meta">${escapeHtml(item.location || "No storage location set")}</p>
            </div>
            <span class="status-badge ${statusClass(itemStatus)}">${escapeHtml(itemStatus)}</span>
          </div>
          <div class="detail-row">
            <div class="detail-item">
              <span class="detail-label">Quantity</span>
              <span class="detail-value">${escapeHtml(formatInventoryQuantity(item))}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Minimum</span>
              <span class="detail-value">${escapeHtml(formatInventoryThreshold(item))}</span>
            </div>
          </div>
          <p class="small-copy">${escapeHtml(item.notes || "No notes added yet.")}</p>
          <div class="row-actions">
            <button class="text-button" type="button" data-inventory-action="edit" data-inventory-id="${item.id}">Edit</button>
            <button class="text-button danger" type="button" data-inventory-action="delete" data-inventory-id="${item.id}">Delete</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderInventoryForm() {
  const fields = elements.inventoryForm.elements;
  const item = state.inventory.find((entry) => entry.id === editingInventoryId) || null;

  if (!item) {
    if (editingInventoryId) {
      editingInventoryId = null;
    }
    if (!fields.namedItem("status").value) {
      fields.namedItem("status").value = "In Stock";
    }
    if (!fields.namedItem("quantity").value) {
      fields.namedItem("quantity").value = "1";
    }
    if (!fields.namedItem("minimumQuantity").value) {
      fields.namedItem("minimumQuantity").value = "0";
    }
    elements.inventorySubmit.textContent = "Add item";
    elements.inventoryCancel.hidden = true;
    return;
  }

  fields.namedItem("name").value = item.name;
  fields.namedItem("location").value = item.location || "";
  fields.namedItem("quantity").value = String(item.quantity ?? 0);
  fields.namedItem("unit").value = item.unit || "";
  fields.namedItem("minimumQuantity").value = String(item.minimumQuantity ?? 0);
  fields.namedItem("status").value = item.status || "In Stock";
  fields.namedItem("notes").value = item.notes || "";
  elements.inventorySubmit.textContent = "Save changes";
  elements.inventoryCancel.hidden = false;
}

function renderExpensesLegacy() {
  const expenses = state.expenses
    .slice()
    .sort((a, b) => String(b.expenseDate || "1900-01-01").localeCompare(String(a.expenseDate || "1900-01-01")));

  if (!expenses.length) {
    elements.expensesList.innerHTML = `<div class="empty-state">No expenses have been logged for ${state.vessel.name} yet.</div>`;
    return;
  }

  elements.expensesList.innerHTML = expenses
    .map(
      (expense) => `
        <article class="maintenance-card expense-card">
          <div class="card-topline">
            <div>
              <h3 class="card-title">${escapeHtml(expense.title)}</h3>
              <p class="card-meta">${formatDate(expense.expenseDate)}${expense.vendor ? ` • ${escapeHtml(expense.vendor)}` : ""}</p>
            </div>
            <span class="priority-badge priority-medium">${escapeHtml(formatCurrency(expense.amount, expense.currency))}</span>
          </div>
          <div class="detail-row">
            <div class="detail-item">
              <span class="detail-label">Category</span>
              <span class="detail-value">${escapeHtml(expense.category || "General")}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">Status</span>
              <span class="detail-value"><span class="status-badge ${statusClass(expense.status || "Planned")}">${escapeHtml(expense.status || "Planned")}</span></span>
            </div>
          </div>
          <p class="small-copy">${escapeHtml(expense.notes || "No notes added yet.")}</p>
          <div class="row-actions">
            <button class="text-button" type="button" data-expense-action="edit" data-expense-id="${expense.id}">Edit</button>
            <button class="text-button danger" type="button" data-expense-action="delete" data-expense-id="${expense.id}">Delete</button>
          </div>
        </article>
      `
    )
    .join("");
}

function renderExpenseForm() {
  const fields = elements.expenseForm.elements;
  const vendorField = fields.namedItem("vendor");
  const categoryField = fields.namedItem("category");
  const expense = state.expenses.find((entry) => entry.id === editingExpenseId) || null;
  vendorField.innerHTML = buildExpenseVendorOptions(expense?.vendor || vendorField.value || "");
  categoryField.innerHTML = buildExpenseCategoryOptions(expense?.category || categoryField.value || "");

  if (!expense) {
    if (editingExpenseId) {
      editingExpenseId = null;
    }
    if (!fields.namedItem("status").value) {
      fields.namedItem("status").value = "Planned";
    }
    if (!fields.namedItem("currency").value) {
      fields.namedItem("currency").value = "USD";
    }
    elements.expenseSubmit.textContent = "Add expense";
    elements.expenseCancel.hidden = true;
    return;
  }

  fields.namedItem("title").value = expense.title;
  vendorField.value = expense.vendor || "";
  categoryField.value = expense.category || "";
  fields.namedItem("amount").value = String(expense.amount ?? 0);
  fields.namedItem("currency").value = expense.currency || "USD";
  fields.namedItem("expenseDate").value = expense.expenseDate || "";
  fields.namedItem("status").value = expense.status || "Planned";
  fields.namedItem("notes").value = expense.notes || "";
  elements.expenseSubmit.textContent = "Save changes";
  elements.expenseCancel.hidden = false;
}

function renderExpenses() {
  const expenses = getSortedExpenses();
  elements.expenseSort.value = state.activeExpenseSort;
  const insights = buildExpenseInsights(expenses);

  renderExpenseAnalytics(insights);

  if (!expenses.length) {
    elements.expensesList.innerHTML = `
      <div class="expense-log-shell">
        <div class="expense-log-heading">
          <div>
            <p class="eyebrow">Expense Log</p>
            <h3>Recent entries</h3>
          </div>
          <p class="small-copy">Start adding vendor invoices, fuel slips, and provisioning spend to build your trail.</p>
        </div>
        <div class="empty-state">No expenses have been logged for ${state.vessel.name} yet.</div>
      </div>
    `;
    return;
  }

  elements.expensesList.innerHTML = `
    <div class="expense-log-shell">
      <div class="expense-log-heading">
        <div>
          <p class="eyebrow">Expense Log</p>
          <h3>Recent entries</h3>
        </div>
        <p class="small-copy">${expenses.length} recorded entries for ${state.vessel.name}.</p>
      </div>
      <div class="expense-log-grid">
        ${expenses
          .map(
            (expense) => `
              <article class="maintenance-card expense-card expense-log-card">
                <div class="card-topline">
                  <div>
                    <h3 class="card-title">${escapeHtml(expense.title)}</h3>
                    <p class="card-meta">${formatDate(expense.expenseDate)}${expense.vendor ? ` | ${escapeHtml(expense.vendor)}` : ""}</p>
                  </div>
                  <span class="priority-badge priority-medium">${escapeHtml(formatCurrency(expense.amount, expense.currency))}</span>
                </div>
                <div class="expense-log-meta">
                  <span class="category-chip ${categoryClass(expense.category || "General")}">${escapeHtml(expense.category || "General")}</span>
                  <span class="status-badge ${statusClass(expense.status || "Planned")}">${escapeHtml(expense.status || "Planned")}</span>
                </div>
                <p class="small-copy">${escapeHtml(expense.notes || "No notes added yet.")}</p>
                <div class="row-actions">
                  <button class="text-button" type="button" data-expense-action="edit" data-expense-id="${expense.id}">Edit</button>
                  <button class="text-button danger" type="button" data-expense-action="delete" data-expense-id="${expense.id}">Delete</button>
                </div>
              </article>
            `
          )
          .join("")}
      </div>
    </div>
  `;
}

function renderExpenseAnalytics(insights) {
  if (!elements.expensesAnalytics) {
    return;
  }

  const topCategories = insights.categories.slice(0, 5);
  const topVendors = insights.vendors.slice(0, 4);
  const maxCategoryTotal = Math.max(1, ...topCategories.map((item) => item.total), 1);
  const maxVendorTotal = Math.max(1, ...topVendors.map((item) => item.total), 1);
  const maxMonthTotal = Math.max(1, ...insights.months.map((item) => item.total), 1);
  const paidShare = insights.totalAmount > 0 ? Math.round((insights.paidTotal / insights.totalAmount) * 100) : 0;

  elements.expensesAnalytics.innerHTML = `
    <div class="expense-dashboard-head">
      <p class="small-copy">Watch spend velocity, payment pipeline, and where money is concentrating across ${state.vessel.name}.</p>
      <span class="expense-currency-note">${escapeHtml(insights.currencyNote)}</span>
    </div>

    <div class="expense-metric-grid">
      <article class="expense-metric-card">
        <span class="metric-label">Total logged</span>
        <strong class="expense-metric-value">${escapeHtml(formatExpenseAmountSummary(insights.totalAmount, insights.reportingCurrency))}</strong>
        <span class="expense-metric-copy">${insights.expenseCount} recorded ${insights.expenseCount === 1 ? "entry" : "entries"}</span>
      </article>
      <article class="expense-metric-card">
        <span class="metric-label">This month</span>
        <strong class="expense-metric-value">${escapeHtml(formatExpenseAmountSummary(insights.currentMonthTotal, insights.reportingCurrency))}</strong>
        <span class="expense-metric-copy">${insights.currentMonthCount} entries in ${escapeHtml(insights.currentMonthLabel)}</span>
      </article>
      <article class="expense-metric-card">
        <span class="metric-label">Awaiting close-out</span>
        <strong class="expense-metric-value">${escapeHtml(formatExpenseAmountSummary(insights.outstandingTotal, insights.reportingCurrency))}</strong>
        <span class="expense-metric-copy">${insights.outstandingCount} not marked paid yet</span>
      </article>
      <article class="expense-metric-card">
        <span class="metric-label">Top vendor</span>
        <strong class="expense-metric-value expense-metric-value-text">${escapeHtml(insights.topVendor?.label || "No vendor yet")}</strong>
        <span class="expense-metric-copy">${escapeHtml(formatExpenseAmountSummary(insights.topVendor?.total || 0, insights.reportingCurrency))}</span>
      </article>
    </div>

    <div class="expense-insight-grid">
      <article class="expense-visual-card expense-trend-card">
        <div class="expense-visual-header">
          <div>
            <p class="eyebrow">Trend</p>
            <h3>Monthly spend</h3>
          </div>
          <span class="expense-visual-caption">Last 6 months</span>
        </div>
        <div class="expense-month-chart">
          ${insights.months
            .map((month, index) => {
              const fillHeight = month.total > 0 ? Math.max(10, (month.total / maxMonthTotal) * 100) : 0;
              return `
                <div class="expense-month-column">
                  <span class="expense-month-value">${escapeHtml(formatExpenseCompactSummary(month.total, insights.reportingCurrency))}</span>
                  <div class="expense-month-track">
                    <div class="expense-month-fill" style="height:${fillHeight}%; background:${EXPENSE_CHART_COLORS[index % EXPENSE_CHART_COLORS.length]};"></div>
                  </div>
                  <span class="expense-month-label">${escapeHtml(month.label)}</span>
                </div>
              `;
            })
            .join("")}
        </div>
      </article>

      <article class="expense-visual-card">
        <div class="expense-visual-header">
          <div>
            <p class="eyebrow">Breakdown</p>
            <h3>Spend by category</h3>
          </div>
          <span class="expense-visual-caption">${escapeHtml(insights.topCategory?.label || "Waiting for spend")}</span>
        </div>
        ${
          topCategories.length
            ? `
              <div class="expense-bar-stack">
                ${topCategories
                  .map((item, index) => `
                    <div class="expense-bar-row">
                      <div class="expense-bar-meta">
                        <span class="expense-bar-label">${escapeHtml(item.label)}</span>
                        <span class="expense-bar-value">${escapeHtml(formatExpenseAmountSummary(item.total, insights.reportingCurrency))}</span>
                      </div>
                      <div class="expense-bar-track">
                        <div class="expense-bar-fill" style="width:${Math.max(8, (item.total / maxCategoryTotal) * 100)}%; background:${EXPENSE_CHART_COLORS[index % EXPENSE_CHART_COLORS.length]};"></div>
                      </div>
                    </div>
                  `)
                  .join("")}
              </div>
            `
            : `<div class="empty-state">Add expenses to unlock category tracking.</div>`
        }
      </article>

      <article class="expense-visual-card">
        <div class="expense-visual-header">
          <div>
            <p class="eyebrow">Status</p>
            <h3>Payment pipeline</h3>
          </div>
          <span class="expense-visual-caption">${paidShare}% paid</span>
        </div>
        <div class="expense-status-layout">
          <div class="expense-status-ring" style="background:${buildExpenseStatusGradient(insights.statuses)};">
            <div class="expense-status-ring-center">
              <span class="metric-label">Paid share</span>
              <strong>${paidShare}%</strong>
            </div>
          </div>
          <div class="expense-status-legend">
            ${insights.statuses
              .map(
                (status) => `
                  <div class="expense-status-row">
                    <span class="expense-status-dot" style="background:${status.color};"></span>
                    <div class="expense-status-copy">
                      <strong>${escapeHtml(status.label)}</strong>
                      <span>${escapeHtml(formatExpenseAmountSummary(status.total, insights.reportingCurrency))} | ${status.count}</span>
                    </div>
                  </div>
                `
              )
              .join("")}
          </div>
        </div>
      </article>

      <article class="expense-visual-card">
        <div class="expense-visual-header">
          <div>
            <p class="eyebrow">Vendors</p>
            <h3>Top spending partners</h3>
          </div>
          <span class="expense-visual-caption">${escapeHtml(insights.topVendor?.label || "No vendors yet")}</span>
        </div>
        ${
          topVendors.length
            ? `
              <div class="expense-bar-stack">
                ${topVendors
                  .map((item, index) => `
                    <div class="expense-bar-row">
                      <div class="expense-bar-meta">
                        <span class="expense-bar-label">${escapeHtml(item.label)}</span>
                        <span class="expense-bar-value">${escapeHtml(formatExpenseAmountSummary(item.total, insights.reportingCurrency))}</span>
                      </div>
                      <div class="expense-bar-track">
                        <div class="expense-bar-fill" style="width:${Math.max(8, (item.total / maxVendorTotal) * 100)}%; background:${EXPENSE_CHART_COLORS[(index + 2) % EXPENSE_CHART_COLORS.length]};"></div>
                      </div>
                    </div>
                  `)
                  .join("")}
              </div>
            `
            : `<div class="empty-state">Your vendor-linked expenses will appear here.</div>`
        }
      </article>
    </div>
  `;
}

function buildExpenseInsights(expenses) {
  const reportingCurrency =
    expenses.find((expense) => String(expense.currency || "").trim())?.currency?.trim().toUpperCase() || "USD";
  const currencyCodes = Array.from(
    new Set(
      expenses
        .map((expense) => String(expense.currency || reportingCurrency).trim().toUpperCase())
        .filter(Boolean)
    )
  );
  const today = parseDateValue(new Date());
  const currentMonthStart = new Date(today.getFullYear(), today.getMonth(), 1);
  const currentMonthLabel = new Intl.DateTimeFormat("en-US", { month: "long" }).format(currentMonthStart);
  const categories = buildExpenseGroupedTotals(expenses, (expense) => expense.category || "General");
  const vendors = buildExpenseGroupedTotals(expenses, (expense) => expense.vendor || "Unassigned vendor");
  const months = buildExpenseMonthBuckets(expenses, 6);
  const statuses = ["Planned", "Submitted", "Approved", "Paid"].map((status) => {
    const matchingExpenses = expenses.filter((expense) => (expense.status || "Planned") === status);
    return {
      label: status,
      total: matchingExpenses.reduce((sum, expense) => sum + getExpenseNumericAmount(expense), 0),
      count: matchingExpenses.length,
      color: getExpenseStatusColor(status),
    };
  });
  const totalAmount = expenses.reduce((sum, expense) => sum + getExpenseNumericAmount(expense), 0);
  const currentMonthExpenses = expenses.filter((expense) => {
    if (!expense.expenseDate) {
      return false;
    }

    const expenseDate = parseDateValue(expense.expenseDate);
    return expenseDate.getFullYear() === currentMonthStart.getFullYear() && expenseDate.getMonth() === currentMonthStart.getMonth();
  });
  const currentMonthTotal = currentMonthExpenses.reduce((sum, expense) => sum + getExpenseNumericAmount(expense), 0);
  const paidTotal = statuses.find((status) => status.label === "Paid")?.total || 0;
  const outstandingStatuses = statuses.filter((status) => status.label !== "Paid");
  const topVendor = vendors.find((vendor) => vendor.label !== "Unassigned vendor") || vendors[0] || null;

  return {
    expenseCount: expenses.length,
    reportingCurrency,
    currencyCodes,
    currencyNote:
      currencyCodes.length > 1
        ? `Mixed currencies in log: ${currencyCodes.join(", ")}. Dashboard totals follow ${reportingCurrency}.`
        : `${reportingCurrency} reporting view`,
    totalAmount,
    paidTotal,
    outstandingTotal: outstandingStatuses.reduce((sum, status) => sum + status.total, 0),
    outstandingCount: outstandingStatuses.reduce((sum, status) => sum + status.count, 0),
    currentMonthTotal,
    currentMonthCount: currentMonthExpenses.length,
    currentMonthLabel,
    categories,
    vendors,
    topCategory: categories[0] || null,
    topVendor,
    months,
    statuses,
  };
}

function buildExpenseGroupedTotals(expenses, getLabel) {
  const grouped = new Map();

  expenses.forEach((expense) => {
    const label = String(getLabel(expense) || "").trim() || "Unassigned";
    const current = grouped.get(label) || {
      label,
      total: 0,
      count: 0,
    };

    current.total += getExpenseNumericAmount(expense);
    current.count += 1;
    grouped.set(label, current);
  });

  return Array.from(grouped.values()).sort((left, right) => right.total - left.total || left.label.localeCompare(right.label));
}

function buildExpenseMonthBuckets(expenses, monthCount = 6) {
  const today = parseDateValue(new Date());
  const buckets = [];

  for (let offset = monthCount - 1; offset >= 0; offset -= 1) {
    const bucketDate = new Date(today.getFullYear(), today.getMonth() - offset, 1);
    buckets.push({
      key: `${bucketDate.getFullYear()}-${String(bucketDate.getMonth() + 1).padStart(2, "0")}`,
      label: new Intl.DateTimeFormat("en-US", { month: "short" }).format(bucketDate),
      total: 0,
      count: 0,
    });
  }

  const bucketMap = new Map(buckets.map((bucket) => [bucket.key, bucket]));
  expenses.forEach((expense) => {
    if (!expense.expenseDate) {
      return;
    }

    const expenseDate = parseDateValue(expense.expenseDate);
    const key = `${expenseDate.getFullYear()}-${String(expenseDate.getMonth() + 1).padStart(2, "0")}`;
    const bucket = bucketMap.get(key);
    if (!bucket) {
      return;
    }

    bucket.total += getExpenseNumericAmount(expense);
    bucket.count += 1;
  });

  return buckets;
}

function getExpenseNumericAmount(expense) {
  const numericAmount = Number(expense?.amount ?? 0);
  return Number.isFinite(numericAmount) ? numericAmount : 0;
}

function buildExpenseStatusGradient(statuses) {
  const activeStatuses = statuses.filter((status) => status.total > 0);
  if (!activeStatuses.length) {
    return "conic-gradient(rgba(255, 255, 255, 0.08) 0deg 360deg)";
  }

  const total = activeStatuses.reduce((sum, status) => sum + status.total, 0);
  let cursor = 0;
  const segments = activeStatuses.map((status, index) => {
    const segment = index === activeStatuses.length - 1 ? 360 - cursor : (status.total / total) * 360;
    const start = cursor;
    cursor += segment;
    return `${status.color} ${start}deg ${cursor}deg`;
  });

  return `conic-gradient(${segments.join(", ")})`;
}

function getExpenseStatusColor(status) {
  switch (status) {
    case "Paid":
      return "#4cb7a5";
    case "Approved":
      return "#b78aff";
    case "Submitted":
      return "#f6c56e";
    case "Planned":
    default:
      return "#6fb7ff";
  }
}

function formatExpenseAmountSummary(amount, currency) {
  return formatCurrency(amount, currency || "USD");
}

function formatExpenseCompactSummary(amount, currency) {
  return formatCompactCurrency(amount, currency || "USD");
}

function renderVoyages() {
  const voyages = getSortedVoyages();
  elements.voyageSort.value = state.activeVoyageSort;

  if (!voyages.length) {
    elements.voyageList.innerHTML = `<div class="empty-state">No voyage entries yet for ${state.vessel.name}.</div>`;
    return;
  }

  elements.voyageList.innerHTML = voyages
    .map(
      (voyage) => `
        <article class="voyage-card">
          <div class="card-topline">
            <div>
              <h3 class="card-title">${voyage.route}</h3>
              <p class="card-meta">Departure ${formatDate(voyage.departure)}</p>
            </div>
            <span class="status-badge ${statusClass(voyage.status)}">${voyage.status}</span>
          </div>
          <p class="small-copy">${voyage.weather}</p>
        </article>
      `
    )
    .join("");
}

function renderAlerts() {
  const alerts = buildAlerts();

  elements.alertFeed.innerHTML = alerts.length
    ? alerts
        .map(
          (alert) => `
            <article class="alert-card">
              <div class="card-topline">
                <h3 class="card-title">${alert.title}</h3>
                <span class="priority-badge ${priorityClass(alert.priority)}">${alert.priority}</span>
              </div>
              <p class="small-copy">${alert.message}</p>
            </article>
          `
        )
        .join("")
    : `<div class="empty-state">No urgent alerts. ${state.vessel.name} looks calm right now.</div>`;
}

function renderViewPanels() {
  ensureAccessibleView();

  elements.workspaceTabs.forEach((tab) => {
    const isActive = tab.dataset.view === state.activeView;
    tab.classList.toggle("active", isActive);
    tab.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  elements.viewPanels.forEach((panel) => {
    const isActive = panel.dataset.viewPanel === state.activeView;
    panel.classList.toggle("active", isActive);
    panel.hidden = !isActive;
  });
}

function buildAlerts() {
  const alerts = [];

  if (state.vessel.fuel <= 25) {
    alerts.push({
      title: "Fuel reserve running low",
      priority: "High",
      message: `${state.vessel.name} fuel reserve is at ${state.vessel.fuel}%. Review bunkering before the next movement.`,
    });
  }

  if (state.vessel.waterTank <= 30) {
    alerts.push({
      title: "Water tank needs attention",
      priority: "Medium",
      message: `${state.vessel.name} fresh water is at ${state.vessel.waterTank}%. Top off before guest or owner use.`,
    });
  }

  if (state.vessel.greyTank >= 75) {
    alerts.push({
      title: "Grey tank nearing capacity",
      priority: "High",
      message: `${state.vessel.name} grey tank is at ${state.vessel.greyTank}%. Plan a pump-out window soon.`,
    });
  }

  if (state.vessel.blackTankLevel >= 75) {
    alerts.push({
      title: "Black water tank nearing capacity",
      priority: "High",
      message: `${state.vessel.name} black water tank is at ${state.vessel.blackTankLevel}%. Schedule discharge before guest use increases.`,
    });
  }

  if (state.vessel.batteryStatus <= 35) {
    alerts.push({
      title: "Battery bank needs attention",
      priority: "Medium",
      message: `${state.vessel.name} battery status is at ${state.vessel.batteryStatus}%. Review charger load and shore power continuity.`,
    });
  }

  [
    { items: state.vessel.engines, typeLabel: "Engine" },
    { items: state.vessel.generators, typeLabel: "Generator" },
  ].forEach(({ items, typeLabel }) => {
    items.forEach((item) => {
      const serviceState = getMachineryServiceState(item);
      if (serviceState.priority === "Low") {
        return;
      }

      const reminderParts = [];
      const hoursRemaining = getMachineryHoursRemaining(item);
      if (hoursRemaining !== null) {
        reminderParts.push(
          hoursRemaining > 0
            ? `${formatNumberValue(hoursRemaining)} hrs remaining before the next interval.`
            : `${formatNumberValue(Math.abs(hoursRemaining))} hrs over the service interval.`
        );
      }

      if (item.nextServiceDate) {
        const daysUntilDateService = dayDifference(todayStamp(), item.nextServiceDate);
        reminderParts.push(
          daysUntilDateService < 0
            ? `Date service was due ${formatDate(item.nextServiceDate)}.`
            : `Date service is scheduled for ${formatDate(item.nextServiceDate)}.`
        );
      }

      if (item.notes) {
        reminderParts.push(item.notes);
      }

      alerts.push({
        title: `${typeLabel} service ${serviceState.label.toLowerCase()}`,
        priority: serviceState.priority,
        message: `${item.label} on ${state.vessel.name}. ${reminderParts.join(" ") || "Review machinery service timing."}`,
      });
    });
  });

  getMaintenanceReminderTasks(state.maintenance)
    .slice(0, 4)
    .forEach((task) => {
      const dueHint = getMaintenanceDueHint(task);
      alerts.push({
        title: isMaintenanceOverdue(task) ? "Overdue maintenance task" : "Maintenance reminder",
        priority: task.priority,
        message: `${task.title} • ${dueHint.detail}. ${task.notes || "No service notes added yet."}`,
      });
    });

  getCurrentWeekWorkOrders()
    .filter((entry) => String(entry.issues || "").trim())
    .slice(0, 3)
    .forEach((entry) => {
      alerts.push({
        title: "Weekly workspace issue logged",
        priority: "Medium",
        message: `${entry.item} on ${formatDate(entry.reportDate)}. ${entry.issues}${entry.notes ? ` Notes: ${entry.notes}` : ""}`,
      });
    });

  state.charters.forEach((charter) => {
    const daysUntilStart = dayDifference(new Date(), charter.start);
    if (daysUntilStart >= 0 && daysUntilStart <= 2) {
      alerts.push({
        title: "Upcoming booking",
        priority: "Medium",
        message: `${charter.client} begins on ${formatDate(charter.start)} at ${charter.berth}.`,
      });
    }
  });

  state.inventory
    .filter((item) => Number(item.quantity || 0) <= Number(item.minimumQuantity || 0))
    .slice(0, 2)
    .forEach((item) => {
      alerts.push({
        title: "Inventory running low",
        priority: "Medium",
        message: `${item.name} is down to ${formatInventoryQuantity(item)} in ${item.location || "storage"}.`,
      });
    });

  state.expenses
    .filter((expense) => (expense.status || "Planned") !== "Paid")
    .slice(0, 2)
    .forEach((expense) => {
      alerts.push({
        title: "Expense awaiting close-out",
        priority: "Low",
        message: `${expense.title} for ${formatCurrency(expense.amount, expense.currency)} is still marked ${expense.status || "Planned"}.`,
      });
    });

  return alerts.slice(0, 4);
}

function getFilteredMaintenanceTasks() {
  let tasks = state.maintenance.slice();
  const resolvedCategory = getResolvedMaintenanceCategory(getMaintenanceCategoryOptions(tasks));

  if (resolvedCategory !== "all") {
    tasks = tasks.filter((task) => task.category === resolvedCategory);
  }

  if (state.activeMaintenanceFilter === "completed") {
    tasks = tasks.filter((task) => isMaintenanceComplete(task));
  }
  else if (state.activeMaintenanceFilter === "overdue") {
    tasks = tasks.filter((task) => isMaintenanceOverdue(task));
  }
  else if (state.activeMaintenanceFilter === "due-soon") {
    tasks = tasks.filter((task) => isMaintenanceDueSoon(task));
  }

  const normalizedQuery = state.activeMaintenanceQuery.trim().toLowerCase();
  if (normalizedQuery) {
    tasks = tasks.filter((task) => taskMatchesMaintenanceQuery(task, normalizedQuery));
  }

  return getSortedMaintenanceTasks(tasks);
}

function compareTextValues(left, right) {
  return String(left || "").localeCompare(String(right || ""), undefined, {
    sensitivity: "base",
    numeric: true,
  });
}

function getPrioritySortRank(priority) {
  switch (String(priority || "").trim().toLowerCase()) {
    case "critical":
      return 0;
    case "high":
      return 1;
    case "medium":
      return 2;
    case "low":
      return 3;
    default:
      return 4;
  }
}

function getWorkOrderStatusRank(order) {
  return isWorkOrderComplete(order) ? 1 : 0;
}

function getMaintenanceStatusRank(task) {
  if (isMaintenanceOverdue(task)) {
    return 0;
  }

  if (task.status === "In Progress") {
    return 1;
  }

  if (isMaintenanceComplete(task)) {
    return 3;
  }

  return 2;
}

function getSortedMaintenanceTasks(tasks, sortMode = state.activeMaintenanceSort) {
  if (sortMode === "due-date-asc") {
    return tasks.slice().sort((a, b) =>
      compareDateStrings(a.dueDate || "9999-12-31", b.dueDate || "9999-12-31")
      || compareTextValues(a.category, b.category)
      || compareTextValues(a.title, b.title)
    );
  }

  if (sortMode === "due-date-desc") {
    return tasks.slice().sort((a, b) =>
      compareDateStrings(b.dueDate || "1900-01-01", a.dueDate || "1900-01-01")
      || compareTextValues(a.category, b.category)
      || compareTextValues(a.title, b.title)
    );
  }

  if (sortMode === "priority") {
    return tasks.slice().sort((a, b) =>
      getPrioritySortRank(a.priority) - getPrioritySortRank(b.priority)
      || getMaintenanceStatusRank(a) - getMaintenanceStatusRank(b)
      || compareDateStrings(a.dueDate || "9999-12-31", b.dueDate || "9999-12-31")
      || compareTextValues(a.title, b.title)
    );
  }

  if (sortMode === "task-asc") {
    return tasks.slice().sort((a, b) =>
      compareTextValues(a.title, b.title)
      || compareDateStrings(a.dueDate || "9999-12-31", b.dueDate || "9999-12-31")
    );
  }

  return tasks.slice().sort((a, b) => {
    const categoryDiff = getMaintenanceCategoryRank(a.category) - getMaintenanceCategoryRank(b.category);
    if (categoryDiff !== 0) {
      return categoryDiff;
    }

    const categoryNameDiff = a.category.localeCompare(b.category);
    if (categoryNameDiff !== 0) {
      return categoryNameDiff;
    }

    const dueDateA = a.dueDate || "9999-12-31";
    const dueDateB = b.dueDate || "9999-12-31";
    const dueDateDiff = dueDateA.localeCompare(dueDateB);
    if (dueDateDiff !== 0) {
      return dueDateDiff;
    }

    return String(a.title || "").localeCompare(String(b.title || ""));
  });
}

function groupMaintenanceTasks(tasks) {
  const groups = new Map();

  tasks.forEach((task) => {
    if (!groups.has(task.category)) {
      groups.set(task.category, []);
    }

    groups.get(task.category).push(task);
  });

  return Array.from(groups.entries()).map(([category, groupTasks]) => ({
    category,
    tasks: groupTasks,
  }));
}

function getMaintenanceGroupStatus(tasks) {
  if (tasks.some((task) => isMaintenanceOverdue(task))) {
    return "Overdue";
  }

  if (tasks.every((task) => isMaintenanceComplete(task)) && !tasks.some((task) => hasMaintenanceUpcomingCycle(task))) {
    return "Completed";
  }

  if (tasks.some((task) => task.status === "In Progress")) {
    return "In Progress";
  }

  return "Not Started";
}

function getMaintenanceDisplayStatus(task) {
  if (isMaintenanceOverdue(task)) {
    return "Overdue";
  }

  if (task.status === "In Progress") {
    return "In Progress";
  }

  if (isMaintenanceComplete(task) && hasMaintenanceUpcomingCycle(task)) {
    return "Scheduled";
  }

  return task.status || "Not Started";
}

function isMaintenanceComplete(task) {
  return task.status === "Completed" || task.status === "Complete";
}

function getMaintenanceDateRemaining(task) {
  if (!usesDateRecurrence(task) || !task?.dueDate) {
    return null;
  }

  return dayDifference(new Date(), task.dueDate);
}

function getMaintenanceSignal(task) {
  const signals = [];
  const dateRemaining = getMaintenanceDateRemaining(task);
  const hourRemaining = getMaintenanceHoursRemaining(task);

  if (dateRemaining !== null) {
    signals.push({ type: "days", remaining: dateRemaining, weight: dateRemaining * 24 });
  }

  if (hourRemaining !== null) {
    signals.push({ type: "hours", remaining: hourRemaining, weight: hourRemaining });
  }

  if (!signals.length) {
    return null;
  }

  const overdueSignals = signals.filter((signal) => signal.remaining < 0);
  if (overdueSignals.length) {
    return overdueSignals.sort((left, right) => Math.abs(left.weight) - Math.abs(right.weight))[0];
  }

  return signals.sort((left, right) => left.weight - right.weight)[0];
}

function isMaintenanceOverdue(task) {
  const signal = getMaintenanceSignal(task);
  return Boolean(signal && signal.remaining < 0);
}

function isMaintenanceDueSoon(task) {
  const dateRemaining = getMaintenanceDateRemaining(task);
  if (dateRemaining !== null && dateRemaining >= 0 && dateRemaining <= getMaintenanceReminderDays(task)) {
    return true;
  }

  const hourRemaining = getMaintenanceHoursRemaining(task);
  return hourRemaining !== null && hourRemaining >= 0 && hourRemaining <= getMaintenanceReminderHours(task);
}

function isMaintenanceDueToday(task) {
  const dateRemaining = getMaintenanceDateRemaining(task);
  if (dateRemaining === 0) {
    return true;
  }

  const hourRemaining = getMaintenanceHoursRemaining(task);
  return hourRemaining !== null && hourRemaining === 0;
}

function hasMaintenanceUpcomingCycle(task) {
  const signal = getMaintenanceSignal(task);
  return isMaintenanceComplete(task) && Boolean(signal && signal.remaining >= 0);
}

function getMaintenanceReminderTasks(tasks) {
  return getSortedMaintenanceTasks(tasks.filter((task) => isMaintenanceReminderActive(task)));
}

function isMaintenanceReminderActive(task) {
  if (isMaintenanceOverdue(task)) {
    return true;
  }

  return isMaintenanceDueSoon(task);
}

function getMaintenanceDueHint(task) {
  const signal = getMaintenanceSignal(task);
  if (!signal) {
    return {
      label: "No due schedule",
      detail: "Set a date or hour trigger",
      toneClass: "is-warn",
    };
  }

  if (signal.remaining < 0) {
    const overdueAmount = Math.abs(signal.remaining);
    return {
      label: signal.type === "hours" ? `Overdue ${overdueAmount} hrs` : `Overdue ${formatDayCount(overdueAmount)}`,
      detail: signal.type === "hours" ? `${overdueAmount} hrs overdue` : `${formatDayCount(overdueAmount)} overdue`,
      toneClass: "is-danger",
    };
  }

  if (signal.remaining === 0) {
    return {
      label: signal.type === "hours" ? "Due at current hours" : "Due today",
      detail: "Service needs attention now",
      toneClass: "is-danger",
    };
  }

  if (signal.type === "hours" && signal.remaining <= getMaintenanceReminderHours(task)) {
    return {
      label: `Reminder ${signal.remaining} hrs`,
      detail: `${signal.remaining} hrs until due`,
      toneClass: "is-warn",
    };
  }

  if (signal.type === "days" && signal.remaining <= getMaintenanceReminderDays(task)) {
    return {
      label: `Reminder ${formatDayCount(signal.remaining)}`,
      detail: `${formatDayCount(signal.remaining)} until due`,
      toneClass: "is-warn",
    };
  }

  return {
    label: signal.type === "hours" ? `Next in ${signal.remaining} hrs` : `Next in ${formatDayCount(signal.remaining)}`,
    detail: signal.type === "hours" ? `Due in ${signal.remaining} hrs` : `Due in ${formatDayCount(signal.remaining)}`,
    toneClass: "is-good",
  };
}

function legacyFormatMaintenanceCadence(task) {
  return `Every ${formatMaintenanceInterval(getMaintenanceIntervalDays(task))} • remind ${formatDayCount(
    getMaintenanceReminderDays(task)
  )} early`;
}

function legacyFormatMaintenanceLastCompletedCopy(task) {
  if (!task?.lastCompleted) {
    return "No service logged yet";
  }

  if (hasMaintenanceUpcomingCycle(task)) {
    return `Next cycle due ${formatOptionalShortDate(task.dueDate)}`;
  }

  return `Reminder window opens ${formatDayCount(getMaintenanceReminderDays(task))} before due`;
}

function legacyFormatMaintenanceInterval(days) {
  if (days % 30 === 0 && days >= 30) {
    const months = days / 30;
    return months === 1 ? "1 month" : `${months} months`;
  }

  if (days % 7 === 0 && days >= 7) {
    const weeks = days / 7;
    return weeks === 1 ? "1 week" : `${weeks} weeks`;
  }

  return formatDayCount(days);
}

function legacyFormatDayCount(days) {
  const safeDays = Math.max(0, Number(days) || 0);
  return `${safeDays} day${safeDays === 1 ? "" : "s"}`;
}

function legacyWasMaintenanceCompletedWithinDays(task, days) {
  if (!task?.lastCompleted) {
    return false;
  }

  const daysSinceCompleted = dayDifference(task.lastCompleted, new Date());
  return daysSinceCompleted >= 0 && daysSinceCompleted <= days;
}

function legacyTaskMatchesMaintenanceQuery(task, normalizedQuery) {
  const haystack = [
    task.title,
    task.category,
    task.notes,
    task.priority,
    task.status,
    task.dueDate,
    task.lastCompleted,
    formatMaintenanceCadence(task),
  ]
    .join(" ")
    .toLowerCase();

  return haystack.includes(normalizedQuery);
}

function legacyGetMaintenanceIntervalDays(task) {
  return normalizePositiveInteger(task?.intervalDays, inferMaintenanceIntervalDays(task));
}

function legacyGetMaintenanceReminderDays(task) {
  return normalizePositiveInteger(task?.reminderDays, inferMaintenanceReminderDays(task), 0);
}

function legacyInferMaintenanceIntervalDays(task) {
  if (task?.lastCompleted && task?.dueDate) {
    const measuredInterval = dayDifference(task.lastCompleted, task.dueDate);
    if (measuredInterval > 0) {
      return measuredInterval;
    }
  }

  if (task?.dueDate) {
    const upcomingInterval = dayDifference(new Date(), task.dueDate);
    if (upcomingInterval > 0) {
      return upcomingInterval;
    }
  }

  return DEFAULT_MAINTENANCE_INTERVAL_DAYS;
}

function legacyInferMaintenanceReminderDays(task) {
  const intervalDays = inferMaintenanceIntervalDays(task);
  if (intervalDays <= 7) {
    return 1;
  }

  if (intervalDays <= 30) {
    return DEFAULT_MAINTENANCE_REMINDER_DAYS;
  }

  if (intervalDays <= 90) {
    return 7;
  }

  return 14;
}

function legacyCompleteMaintenanceTask(task) {
  const completedOn = todayStamp();
  task.status = "Completed";
  task.lastCompleted = completedOn;
  task.dueDate = addDaysToDate(completedOn, getMaintenanceIntervalDays(task));
}

function formatMaintenanceCadence(task) {
  const cadenceParts = [];
  const reminderParts = [];

  if (usesDateRecurrence(task) && getMaintenanceIntervalDays(task) > 0) {
    cadenceParts.push(`Every ${formatMaintenanceInterval(getMaintenanceIntervalDays(task))}`);
  }

  if (usesHourRecurrence(task) && getMaintenanceIntervalHours(task) > 0) {
    cadenceParts.push(`Every ${formatHourCount(getMaintenanceIntervalHours(task))}`);
  }

  if (usesDateRecurrence(task) && getMaintenanceReminderDays(task) > 0) {
    reminderParts.push(`${formatDayCount(getMaintenanceReminderDays(task))} early`);
  }

  if (usesHourRecurrence(task) && getMaintenanceReminderHours(task) > 0) {
    reminderParts.push(`${formatHourCount(getMaintenanceReminderHours(task))} early`);
  }

  if (!cadenceParts.length) {
    cadenceParts.push("Manual cadence");
  }

  return reminderParts.length
    ? `${cadenceParts.join(" or ")} | remind ${reminderParts.join(" or ")}`
    : cadenceParts.join(" or ");
}

function formatMaintenanceLastCompletedCopy(task) {
  if (!task?.lastCompleted && !task?.lastCompletedHours) {
    return "No service logged yet";
  }

  const signal = getMaintenanceSignal(task);
  if (signal && signal.remaining >= 0) {
    return signal.type === "hours"
      ? `Next cycle due at ${formatHourCount(getMaintenanceDueHours(task))}`
      : `Next cycle due ${formatOptionalShortDate(task.dueDate)}`;
  }

  const reminderParts = [];
  if (usesDateRecurrence(task) && getMaintenanceReminderDays(task) > 0) {
    reminderParts.push(formatDayCount(getMaintenanceReminderDays(task)));
  }
  if (usesHourRecurrence(task) && getMaintenanceReminderHours(task) > 0) {
    reminderParts.push(formatHourCount(getMaintenanceReminderHours(task)));
  }

  return reminderParts.length
    ? `Reminder window opens ${reminderParts.join(" or ")} before due`
    : "No reminder window set";
}

function formatMaintenanceInterval(days) {
  if (days % 30 === 0 && days >= 30) {
    const months = days / 30;
    return months === 1 ? "1 month" : `${months} months`;
  }

  if (days % 7 === 0 && days >= 7) {
    const weeks = days / 7;
    return weeks === 1 ? "1 week" : `${weeks} weeks`;
  }

  return formatDayCount(days);
}

function formatDayCount(days) {
  const safeDays = Math.max(0, Number(days) || 0);
  return `${safeDays} day${safeDays === 1 ? "" : "s"}`;
}

function formatHourCount(hours) {
  const safeHours = Math.max(0, Number(hours) || 0);
  return `${formatNumberValue(safeHours)} hr${safeHours === 1 ? "" : "s"}`;
}

function wasMaintenanceCompletedWithinDays(task, days) {
  if (!task?.lastCompleted) {
    return false;
  }

  const daysSinceCompleted = dayDifference(task.lastCompleted, new Date());
  return daysSinceCompleted >= 0 && daysSinceCompleted <= days;
}

function wasDateWithinDays(dateValue, days) {
  if (!dateValue) {
    return false;
  }

  const daysSince = dayDifference(dateValue, new Date());
  return daysSince >= 0 && daysSince <= Math.max(0, Number(days) || 0);
}

function taskMatchesMaintenanceQuery(task, normalizedQuery) {
  const linkedAsset = getMaintenanceAssetById(task.assetId);
  const linkedTemplate = getMaintenanceTemplateById(task.templateId);
  const haystack = [
    task.title,
    task.category,
    task.notes,
    task.priority,
    task.status,
    task.dueDate,
    task.lastCompleted,
    formatMaintenanceCadence(task),
    formatMaintenanceDueValue(task),
    formatMaintenanceLastCompletedValue(task),
    getMaintenanceMeterSummary(task),
    linkedAsset?.name,
    linkedAsset?.manufacturer,
    linkedAsset?.model,
    linkedAsset?.location,
    linkedTemplate?.name,
  ]
    .join(" ")
    .toLowerCase();

  return haystack.includes(normalizedQuery);
}

function getMaintenanceIntervalDays(task) {
  return normalizePositiveInteger(task?.intervalDays, inferMaintenanceIntervalDays(task));
}

function getMaintenanceReminderDays(task) {
  return normalizePositiveInteger(task?.reminderDays, inferMaintenanceReminderDays(task), 0);
}

function inferMaintenanceIntervalDays(task) {
  if (task?.lastCompleted && task?.dueDate) {
    const measuredInterval = dayDifference(task.lastCompleted, task.dueDate);
    if (measuredInterval > 0) {
      return measuredInterval;
    }
  }

  if (task?.dueDate) {
    const upcomingInterval = dayDifference(new Date(), task.dueDate);
    if (upcomingInterval > 0) {
      return upcomingInterval;
    }
  }

  return DEFAULT_MAINTENANCE_INTERVAL_DAYS;
}

function inferMaintenanceIntervalHours(task) {
  const lastCompletedHours = normalizePositiveInteger(task?.lastCompletedHours ?? task?.last_completed_hours, 0, 0);
  const dueHours = normalizePositiveInteger(task?.dueHours ?? task?.due_hours, 0, 0);

  if (lastCompletedHours > 0 && dueHours > lastCompletedHours) {
    return dueHours - lastCompletedHours;
  }

  if (dueHours > 0) {
    const currentHours = getMaintenanceCurrentHours(task);
    const projectedInterval = dueHours - currentHours;
    if (projectedInterval > 0) {
      return projectedInterval;
    }
  }

  return DEFAULT_MAINTENANCE_INTERVAL_HOURS;
}

function inferMaintenanceReminderDays(task) {
  const intervalDays = inferMaintenanceIntervalDays(task);
  if (intervalDays <= 7) {
    return 1;
  }

  if (intervalDays <= 30) {
    return DEFAULT_MAINTENANCE_REMINDER_DAYS;
  }

  if (intervalDays <= 90) {
    return 7;
  }

  return 14;
}

function inferMaintenanceReminderHours(task) {
  const intervalHours = inferMaintenanceIntervalHours(task);
  if (intervalHours <= 0) {
    return DEFAULT_MAINTENANCE_REMINDER_HOURS;
  }

  if (intervalHours <= 25) {
    return 1;
  }

  if (intervalHours <= 100) {
    return 5;
  }

  if (intervalHours <= 250) {
    return 10;
  }

  return 25;
}

function formatMaintenanceDueValue(task, compact = false) {
  const parts = [];

  if (usesDateRecurrence(task) && task?.dueDate) {
    parts.push(compact ? formatOptionalShortDate(task.dueDate) : formatDate(task.dueDate));
  }

  if (usesHourRecurrence(task) && getMaintenanceDueHours(task) > 0) {
    parts.push(formatHourCount(getMaintenanceDueHours(task)));
  }

  if (!parts.length) {
    return compact ? "No due schedule" : "Set a date or hour trigger";
  }

  return parts.join(" | ");
}

function formatMaintenanceLastCompletedValue(task, compact = false) {
  const parts = [];

  if (usesDateRecurrence(task) && task?.lastCompleted) {
    parts.push(compact ? formatOptionalShortDate(task.lastCompleted) : formatDate(task.lastCompleted));
  }

  if (usesHourRecurrence(task) && normalizePositiveInteger(task?.lastCompletedHours, 0, 0) > 0) {
    parts.push(formatHourCount(task.lastCompletedHours));
  }

  if (!parts.length) {
    return compact ? "Not logged" : "No service logged";
  }

  return parts.join(" | ");
}

function getMaintenanceMeterSummary(task) {
  const linkedAsset = getMaintenanceAssetById(task?.assetId);
  const meter = getMaintenanceTaskMeterConfig(task);
  const summaryParts = [];

  if (linkedAsset?.name) {
    summaryParts.push(linkedAsset.name);
  }

  if (usesHourRecurrence(task)) {
    const currentHours = getMaintenanceCurrentHours(task);
    const sourceLabel = meter.meterSourceType === "engine"
      ? "linked engine"
      : meter.meterSourceType === "generator"
        ? "linked generator"
        : "manual hour meter";
    summaryParts.push(`${sourceLabel}: ${formatHourCount(currentHours)}`);
  } else {
    summaryParts.push("Date-based service");
  }

  return summaryParts.join(" | ");
}

function getMaintenanceTaskSortValue(task) {
  const signal = getMaintenanceSignal(task);
  if (!signal) {
    return Number.MAX_SAFE_INTEGER;
  }

  return signal.type === "hours" ? signal.remaining : signal.remaining * 24;
}

function completeMaintenanceTask(task) {
  const completedOn = todayStamp();
  const completedHours = usesHourRecurrence(task) ? getMaintenanceCurrentHours(task) : 0;
  task.status = "Completed";
  task.updatedAt = currentIsoStamp();

  if (usesDateRecurrence(task)) {
    task.lastCompleted = completedOn;
    task.dueDate = getMaintenanceIntervalDays(task) > 0
      ? addDaysToDate(completedOn, getMaintenanceIntervalDays(task))
      : "";
  }

  if (usesHourRecurrence(task)) {
    task.lastCompletedHours = completedHours;
    task.dueHours = getMaintenanceIntervalHours(task) > 0
      ? completedHours + getMaintenanceIntervalHours(task)
      : 0;
  }

  state.maintenanceHistory = normalizeMaintenanceHistoryCollection(
    [
      normalizeMaintenanceHistoryEntry({
        id: createId("maintenance-history"),
        vesselId: state.activeVesselId,
        maintenanceLogId: task.id,
        assetId: task.assetId,
        templateTaskId: task.templateTaskId,
        workOrderId: "",
        source: "manual",
        completedAt: currentIsoStamp(),
        completionDate: completedOn,
        completedHours,
        workDone: task.title,
        systemsChecked: "",
        issues: "",
        notes: task.notes,
      }),
      ...state.maintenanceHistory,
    ],
    []
  );
}

function getMaintenanceCategoryRank(category) {
  const rank = MAINTENANCE_CATEGORY_ORDER.indexOf(category);
  return rank === -1 ? MAINTENANCE_CATEGORY_ORDER.length : rank;
}

function getMaintenanceCategoryOptions(tasks) {
  const categories = [];

  MAINTENANCE_CATEGORY_ORDER.forEach((category) => {
    if (tasks.some((task) => task.category === category)) {
      categories.push(category);
    }
  });

  tasks
    .map((task) => task.category)
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right))
    .forEach((category) => {
      if (!categories.includes(category)) {
        categories.push(category);
      }
    });

  return categories;
}

function getDefaultMaintenanceCategory(tasks) {
  const categories = getMaintenanceCategoryOptions(tasks);
  return categories[0] || "all";
}

function getResolvedMaintenanceCategory(categories) {
  if (state.activeMaintenanceCategory === "all") {
    return "all";
  }

  if (categories.includes(state.activeMaintenanceCategory)) {
    return state.activeMaintenanceCategory;
  }

  return getDefaultMaintenanceCategory(state.maintenance);
}

function isWorkOrderComplete(order) {
  return order.status === "Completed";
}

function getFilteredVendors() {
  if (state.activeVendorFilter === "all") {
    return state.vendors;
  }

  if (state.activeVendorFilter === "active") {
    return state.vendors.filter((vendor) => vendor.status === "Active");
  }

  if (state.activeVendorFilter === "under-review") {
    return state.vendors.filter((vendor) => vendor.status === "Under review");
  }

  return state.vendors;
}

function buildExpenseVendorOptions(selectedVendorName = "") {
  const selectedName = String(selectedVendorName || "").trim();
  const vendors = state.vendors
    .slice()
    .sort(
      (left, right) =>
        String(left.category || "").localeCompare(String(right.category || "")) ||
        String(left.name || "").localeCompare(String(right.name || ""))
    );

  const groupedVendors = vendors.reduce((groups, vendor) => {
    const category = String(vendor.category || "Uncategorized").trim() || "Uncategorized";
    if (!groups.has(category)) {
      groups.set(category, []);
    }
    groups.get(category).push(vendor);
    return groups;
  }, new Map());

  const optionGroups = Array.from(groupedVendors.entries())
    .map(
      ([category, categoryVendors]) => `
        <optgroup label="${escapeHtml(category)}">
          ${categoryVendors
            .map(
              (vendor) =>
                `<option value="${escapeHtml(vendor.name)}">${escapeHtml(vendor.name)}</option>`
            )
            .join("")}
        </optgroup>
      `
    )
    .join("");

  const missingSelectedOption =
    selectedName && !findVendorByName(selectedName)
      ? `<option value="${escapeHtml(selectedName)}">${escapeHtml(selectedName)} (saved on expense)</option>`
      : "";

  return `
    <option value="">Select saved vendor</option>
    ${missingSelectedOption}
    ${optionGroups}
  `;
}

function buildExpenseCategoryOptions(selectedCategory = "") {
  const selectedName = String(selectedCategory || "").trim();
  const categories = Array.from(
    new Set(
      state.vendors
        .map((vendor) => String(vendor.category || "").trim())
        .filter(Boolean)
        .sort((left, right) => left.localeCompare(right))
    )
  );

  const missingSelectedOption =
    selectedName && !categories.some((category) => category.toLowerCase() === selectedName.toLowerCase())
      ? `<option value="${escapeHtml(selectedName)}">${escapeHtml(selectedName)} (saved on expense)</option>`
      : "";

  return `
    <option value="">Select saved category</option>
    ${missingSelectedOption}
    ${categories.map((category) => `<option value="${escapeHtml(category)}">${escapeHtml(category)}</option>`).join("")}
  `;
}

function findVendorByName(vendorName) {
  const normalizedVendorName = String(vendorName || "").trim().toLowerCase();
  if (!normalizedVendorName) {
    return null;
  }

  return (
    state.vendors.find((vendor) => String(vendor.name || "").trim().toLowerCase() === normalizedVendorName) ||
    null
  );
}

function createId(prefix) {
  return `${prefix}-${Math.random().toString(16).slice(2, 8)}`;
}

function isValidDateObject(value) {
  return value instanceof Date && !Number.isNaN(value.getTime());
}

function formatDate(dateString) {
  const parsedDate = parseDateValue(dateString);
  if (!isValidDateObject(parsedDate)) {
    return "--";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(parsedDate);
}

function formatDateTime(dateString) {
  const parsedDate = parseDateValue(dateString);
  if (!isValidDateObject(parsedDate)) {
    return "--";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(parsedDate);
}

function formatCurrency(amount, currency = "USD") {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return `${currency || "USD"} 0.00`;
  }

  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currency || "USD",
    }).format(numericAmount);
  } catch {
    return `${currency || "USD"} ${numericAmount.toFixed(2)}`;
  }
}

function formatCompactCurrency(amount, currency = "USD") {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return `${currency || "USD"} 0`;
  }

  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currency || "USD",
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(numericAmount);
  } catch {
    const compactValue = new Intl.NumberFormat("en-US", {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(numericAmount);
    return `${currency || "USD"} ${compactValue}`;
  }
}

function formatInventoryQuantity(item) {
  const quantity = Number(item?.quantity ?? 0);
  const safeQuantity = Number.isFinite(quantity) ? quantity : 0;
  const unit = String(item?.unit || "").trim();
  return unit ? `${safeQuantity} ${unit}` : `${safeQuantity}`;
}

function formatInventoryThreshold(item) {
  const minimumQuantity = Number(item?.minimumQuantity ?? 0);
  const safeMinimum = Number.isFinite(minimumQuantity) ? minimumQuantity : 0;
  const unit = String(item?.unit || "").trim();
  return unit ? `${safeMinimum} ${unit}` : `${safeMinimum}`;
}

function formatOptionalDate(dateString) {
  return dateString ? formatDate(dateString) : '<span class="log-empty">-</span>';
}

function formatOptionalShortDate(dateString) {
  return dateString ? formatDate(dateString) : "No date set";
}

function formatInputDate(date) {
  const normalizedDate = parseDateValue(date);
  if (!isValidDateObject(normalizedDate)) {
    return todayStamp();
  }

  const year = normalizedDate.getFullYear();
  const month = String(normalizedDate.getMonth() + 1).padStart(2, "0");
  const day = String(normalizedDate.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function parseDateValue(value) {
  if (value instanceof Date) {
    if (isValidDateObject(value)) {
      return new Date(value.getFullYear(), value.getMonth(), value.getDate());
    }
    return new Date();
  }

  if (typeof value === "string") {
    const dateParts = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
    if (dateParts) {
      const localDate = new Date(Number(dateParts[1]), Number(dateParts[2]) - 1, Number(dateParts[3]));
      if (isValidDateObject(localDate)) {
        return localDate;
      }
      return new Date();
    }
  }

  const parsed = new Date(value);
  if (isValidDateObject(parsed)) {
    return new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
  }

  return new Date();
}

function formatReportDate(dateString) {
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  }).format(parseDateValue(dateString));
}

function formatReportPeriod(reportWindow) {
  const weekStart = reportWindow?.weekStart || reportWindow?.start || todayStamp();
  const weekEnd = reportWindow?.weekEnd || reportWindow?.end || weekStart;
  return `WEEKLY LOG ${formatMonthDay(weekStart)} - ${formatMonthDay(weekEnd)}`;
}

function formatMonthDay(date) {
  return new Intl.DateTimeFormat("en-US", {
    month: "numeric",
    day: "numeric",
  }).format(parseDateValue(date));
}

function formatWeatherUpdated(dateString) {
  const parsedDate = new Date(dateString);
  if (!isValidDateObject(parsedDate)) {
    return "Updated recently";
  }

  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    hour: "numeric",
    minute: "2-digit",
  }).format(parsedDate);
}

function compareDateStrings(left, right) {
  return formatInputDate(left).localeCompare(formatInputDate(right));
}

function getWorkWeekRange(referenceDate = new Date()) {
  const currentDate = parseDateValue(referenceDate);
  const dayOfWeek = currentDate.getDay();
  const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
  const start = new Date(currentDate);
  start.setDate(currentDate.getDate() + mondayOffset);

  const end = new Date(start);
  end.setDate(start.getDate() + 4);

  return {
    start: formatInputDate(start),
    end: formatInputDate(end),
  };
}

function getCurrentReportWindow(referenceDate = new Date()) {
  const weekRange = getWorkWeekRange(referenceDate);
  return {
    start: weekRange.start,
    end: weekRange.end,
  };
}

function isDateWithinRange(dateValue, start, end) {
  const current = parseDateValue(dateValue);
  return current >= parseDateValue(start) && current <= parseDateValue(end);
}

function getPreferredCurrentWorkOrderDate() {
  const currentWeek = getActiveWorkWeekRange();
  const today = todayStamp();
  if (compareDateStrings(today, currentWeek.start) < 0) {
    return currentWeek.start;
  }
  if (compareDateStrings(today, currentWeek.end) > 0) {
    return currentWeek.end;
  }
  return today;
}

function getSortedWorkOrders(items = state.workOrders, sortMode = state.activeWorkOrderSort) {
  const normalizedItems = normalizeWorkOrderCollection(items, []).slice();

  if (sortMode === "date-desc") {
    return normalizedItems.sort((left, right) =>
      compareDateStrings(right.reportDate || right.dueDate || todayStamp(), left.reportDate || left.dueDate || todayStamp())
      || Number(left.sortOrder || 0) - Number(right.sortOrder || 0)
      || compareTextValues(left.item || left.title || "", right.item || right.title || "")
    );
  }

  if (sortMode === "task-asc") {
    return normalizedItems.sort((left, right) =>
      compareTextValues(left.item || left.title || "", right.item || right.title || "")
      || compareDateStrings(left.reportDate || left.dueDate || todayStamp(), right.reportDate || right.dueDate || todayStamp())
    );
  }

  if (sortMode === "task-desc") {
    return normalizedItems.sort((left, right) =>
      compareTextValues(right.item || right.title || "", left.item || left.title || "")
      || compareDateStrings(left.reportDate || left.dueDate || todayStamp(), right.reportDate || right.dueDate || todayStamp())
    );
  }

  if (sortMode === "status") {
    return normalizedItems.sort((left, right) =>
      getWorkOrderStatusRank(left) - getWorkOrderStatusRank(right)
      || compareDateStrings(left.reportDate || left.dueDate || todayStamp(), right.reportDate || right.dueDate || todayStamp())
      || compareTextValues(left.item || left.title || "", right.item || right.title || "")
    );
  }

  return normalizedItems.sort((left, right) =>
    compareDateStrings(left.reportDate || left.dueDate || todayStamp(), right.reportDate || right.dueDate || todayStamp())
    || Number(left.sortOrder || 0) - Number(right.sortOrder || 0)
    || compareTextValues(left.item || left.title || "", right.item || right.title || "")
  );
}

function getCurrentWeekWorkOrders(items = state.workOrders, referenceDate = null) {
  const currentWeek = referenceDate === null || typeof referenceDate === "undefined"
    ? getActiveWorkWeekRange()
    : getCurrentWeekRange(referenceDate);
  return getSortedWorkOrders(items).filter((entry) => {
    const weekStart = entry.weekStart || currentWeek.start;
    const weekEnd = entry.weekEnd || currentWeek.end;
    const reportDate = entry.reportDate || entry.dueDate || currentWeek.start;
    return (
      (weekStart === currentWeek.start && weekEnd === currentWeek.end)
      || isDateWithinRange(reportDate, currentWeek.start, currentWeek.end)
    );
  });
}

function getCurrentWeekGeneratedReport(referenceDate = null) {
  const currentWeek = referenceDate === null || typeof referenceDate === "undefined"
    ? getActiveWorkWeekRange()
    : getCurrentWeekRange(referenceDate);
  return state.reports.find((report) => report.weekStart === currentWeek.start && report.weekEnd === currentWeek.end) || null;
}

function getSortedVendors(items = getFilteredVendors(), sortMode = state.activeVendorSort) {
  const vendors = items.slice();

  if (sortMode === "name-desc") {
    return vendors.sort((left, right) => compareTextValues(right.name, left.name));
  }

  if (sortMode === "category") {
    return vendors.sort((left, right) =>
      compareTextValues(left.category, right.category)
      || compareTextValues(left.name, right.name)
    );
  }

  if (sortMode === "status") {
    return vendors.sort((left, right) =>
      compareTextValues(left.status, right.status)
      || compareTextValues(left.name, right.name)
    );
  }

  return vendors.sort((left, right) => compareTextValues(left.name, right.name));
}

function getSortedInventoryItems(items = state.inventory, sortMode = state.activeInventorySort) {
  const normalizedItems = items.slice();

  if (sortMode === "name-desc") {
    return normalizedItems.sort((left, right) => compareTextValues(right.name, left.name));
  }

  if (sortMode === "quantity-low") {
    return normalizedItems.sort((left, right) =>
      Number(left.quantity || 0) - Number(right.quantity || 0)
      || compareTextValues(left.name, right.name)
    );
  }

  if (sortMode === "quantity-high") {
    return normalizedItems.sort((left, right) =>
      Number(right.quantity || 0) - Number(left.quantity || 0)
      || compareTextValues(left.name, right.name)
    );
  }

  if (sortMode === "location") {
    return normalizedItems.sort((left, right) =>
      compareTextValues(left.location, right.location)
      || compareTextValues(left.name, right.name)
    );
  }

  return normalizedItems.sort((left, right) => compareTextValues(left.name, right.name));
}

function getSortedExpenses(items = state.expenses, sortMode = state.activeExpenseSort) {
  const expenses = items.slice();

  if (sortMode === "date-asc") {
    return expenses.sort((a, b) =>
      compareDateStrings(a.expenseDate || "1900-01-01", b.expenseDate || "1900-01-01")
      || compareTextValues(a.title, b.title)
    );
  }

  if (sortMode === "amount-desc") {
    return expenses.sort((a, b) =>
      getExpenseNumericAmount(b) - getExpenseNumericAmount(a)
      || compareDateStrings(String(b.expenseDate || "1900-01-01"), String(a.expenseDate || "1900-01-01"))
      || compareTextValues(a.title, b.title)
    );
  }

  if (sortMode === "amount-asc") {
    return expenses.sort((a, b) =>
      getExpenseNumericAmount(a) - getExpenseNumericAmount(b)
      || compareDateStrings(String(b.expenseDate || "1900-01-01"), String(a.expenseDate || "1900-01-01"))
      || compareTextValues(a.title, b.title)
    );
  }

  if (sortMode === "vendor") {
    return expenses.sort((a, b) =>
      compareTextValues(a.vendor, b.vendor)
      || compareDateStrings(String(b.expenseDate || "1900-01-01"), String(a.expenseDate || "1900-01-01"))
      || compareTextValues(a.title, b.title)
    );
  }

  return expenses.sort((a, b) =>
    compareDateStrings(String(b.expenseDate || "1900-01-01"), String(a.expenseDate || "1900-01-01"))
    || compareTextValues(a.title, b.title)
  );
}

function getSortedCharters(items = state.charters, sortMode = state.activeCharterSort) {
  const charters = items.slice();

  if (sortMode === "start-desc") {
    return charters.sort((a, b) =>
      compareDateStrings(b.start || "1900-01-01", a.start || "1900-01-01")
      || compareTextValues(a.client, b.client)
    );
  }

  if (sortMode === "client-asc") {
    return charters.sort((a, b) =>
      compareTextValues(a.client, b.client)
      || compareDateStrings(a.start || "1900-01-01", b.start || "1900-01-01")
    );
  }

  if (sortMode === "status") {
    return charters.sort((a, b) =>
      compareTextValues(a.status, b.status)
      || compareDateStrings(a.start || "1900-01-01", b.start || "1900-01-01")
      || compareTextValues(a.client, b.client)
    );
  }

  return charters.sort((a, b) =>
    compareDateStrings(a.start || "1900-01-01", b.start || "1900-01-01")
    || compareTextValues(a.client, b.client)
  );
}

function getSortedVoyages(items = state.voyages, sortMode = state.activeVoyageSort) {
  const voyages = items.slice();

  if (sortMode === "departure-desc") {
    return voyages.sort((a, b) =>
      compareDateStrings(b.departure || "1900-01-01", a.departure || "1900-01-01")
      || compareTextValues(a.route, b.route)
    );
  }

  if (sortMode === "route-asc") {
    return voyages.sort((a, b) =>
      compareTextValues(a.route, b.route)
      || compareDateStrings(a.departure || "1900-01-01", b.departure || "1900-01-01")
    );
  }

  if (sortMode === "status") {
    return voyages.sort((a, b) =>
      compareTextValues(a.status, b.status)
      || compareDateStrings(a.departure || "1900-01-01", b.departure || "1900-01-01")
      || compareTextValues(a.route, b.route)
    );
  }

  return voyages.sort((a, b) =>
    compareDateStrings(a.departure || "1900-01-01", b.departure || "1900-01-01")
    || compareTextValues(a.route, b.route)
  );
}

function renderLogValue(value) {
  const safeValue = typeof value === "string" ? value : "";
  return safeValue.trim().length ? escapeHtml(safeValue) : '<span class="log-empty">-</span>';
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function addDays(days) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return formatInputDate(date);
}

function addDaysToDate(dateValue, days) {
  const date = parseDateValue(dateValue);
  date.setDate(date.getDate() + Number(days || 0));
  return formatInputDate(date);
}

function todayStamp() {
  return formatInputDate(new Date());
}

function seedFormDates() {
  const maintenanceDue = elements.maintenanceForm.elements.namedItem("dueDate");
  const maintenanceInterval = elements.maintenanceForm.elements.namedItem("intervalDays");
  const maintenanceIntervalHours = elements.maintenanceForm.elements.namedItem("intervalHours");
  const maintenanceReminder = elements.maintenanceForm.elements.namedItem("reminderDays");
  const maintenanceReminderHours = elements.maintenanceForm.elements.namedItem("reminderHours");
  const maintenanceDueHours = elements.maintenanceForm.elements.namedItem("dueHours");
  const maintenanceLastCompletedHours = elements.maintenanceForm.elements.namedItem("lastCompletedHours");
  const workOrderDate = elements.workOrderForm.elements.namedItem("reportDate");
  const charterStart = elements.charterForm.elements.namedItem("start");
  const charterEnd = elements.charterForm.elements.namedItem("end");

  if (maintenanceDue && !maintenanceDue.value) {
    maintenanceDue.value = addDays(7);
  }

  if (maintenanceInterval && !maintenanceInterval.value) {
    maintenanceInterval.value = String(DEFAULT_MAINTENANCE_INTERVAL_DAYS);
  }

  if (maintenanceReminder && !maintenanceReminder.value) {
    maintenanceReminder.value = String(DEFAULT_MAINTENANCE_REMINDER_DAYS);
  }

  if (maintenanceIntervalHours && !maintenanceIntervalHours.value) {
    maintenanceIntervalHours.value = String(DEFAULT_MAINTENANCE_INTERVAL_HOURS);
  }

  if (maintenanceReminderHours && !maintenanceReminderHours.value) {
    maintenanceReminderHours.value = String(DEFAULT_MAINTENANCE_REMINDER_HOURS);
  }

  if (maintenanceDueHours && !maintenanceDueHours.value) {
    maintenanceDueHours.value = "0";
  }

  if (maintenanceLastCompletedHours && !maintenanceLastCompletedHours.value) {
    maintenanceLastCompletedHours.value = "0";
  }

  if (workOrderDate && !workOrderDate.value) {
    workOrderDate.value = getPreferredCurrentWorkOrderDate();
  }

  if (charterStart && !charterStart.value) {
    charterStart.value = addDays(3);
  }

  if (charterEnd && !charterEnd.value) {
    charterEnd.value = addDays(6);
  }

  const expenseDate = elements.expenseForm?.elements?.namedItem("expenseDate");
  if (expenseDate && !expenseDate.value) {
    expenseDate.value = addDays(0);
  }
}

function resetMaintenanceAssetForm() {
  editingMaintenanceAssetId = null;
  elements.maintenanceAssetForm.reset();
  const fields = elements.maintenanceAssetForm.elements;
  fields.namedItem("templateId").innerHTML = buildMaintenanceTemplateOptions("");
  fields.namedItem("templateId").value = "";
  fields.namedItem("meterSourceType").value = "none";
  fields.namedItem("meterSourceId").innerHTML = buildMaintenanceMeterSourceOptions("none", "");
  fields.namedItem("meterSourceId").value = "";
  fields.namedItem("currentHours").value = "0";
  elements.maintenanceAssetSubmit.textContent = "Install asset";
  elements.maintenanceAssetCancel.hidden = true;
  renderMaintenanceTemplatePreview(null);
}

function resetMaintenanceForm() {
  editingMaintenanceId = null;
  elements.maintenanceForm.reset();
  seedFormDates();
  const fields = elements.maintenanceForm.elements;
  const preferredCategory = getResolvedMaintenanceCategory(getMaintenanceCategoryOptions(state.maintenance));
  if (preferredCategory !== "all") {
    fields.namedItem("category").value = preferredCategory;
  }
  fields.namedItem("assetId").innerHTML = buildMaintenanceAssetOptions("");
  fields.namedItem("assetId").value = "";
  fields.namedItem("status").value = "Not Started";
  fields.namedItem("priority").value = "High";
  fields.namedItem("recurrenceMode").value = "days";
  fields.namedItem("intervalDays").value = String(DEFAULT_MAINTENANCE_INTERVAL_DAYS);
  fields.namedItem("intervalHours").value = String(DEFAULT_MAINTENANCE_INTERVAL_HOURS);
  fields.namedItem("reminderDays").value = String(DEFAULT_MAINTENANCE_REMINDER_DAYS);
  fields.namedItem("reminderHours").value = String(DEFAULT_MAINTENANCE_REMINDER_HOURS);
  fields.namedItem("dueHours").value = "0";
  fields.namedItem("lastCompletedHours").value = "0";
  fields.namedItem("meterSourceType").value = "none";
  fields.namedItem("meterSourceId").innerHTML = buildMaintenanceMeterSourceOptions("none", "");
  fields.namedItem("meterSourceId").value = "";
  elements.maintenanceSubmit.textContent = "Add task";
  elements.maintenanceCancel.hidden = true;
}

function resetWorkOrderForm() {
  editingWorkOrderId = null;
  elements.workOrderForm.reset();
  seedFormDates();
  elements.workOrderForm.elements.namedItem("reportDate").value = getPreferredCurrentWorkOrderDate();
  elements.workOrderForm.elements.namedItem("maintenanceLogId").value = "";
  elements.workOrderSubmit.textContent = "Add entry";
  elements.workOrderCancel.hidden = true;
}

function resetEngineForm() {
  editingEngineId = null;
  elements.engineForm.reset();
  elements.engineForm.elements.namedItem("hours").value = "0";
  elements.engineForm.elements.namedItem("serviceIntervalHours").value = "0";
  elements.engineSubmit.textContent = "Add engine";
  elements.engineCancel.hidden = true;
}

function resetGeneratorForm() {
  editingGeneratorId = null;
  elements.generatorForm.reset();
  elements.generatorForm.elements.namedItem("hours").value = "0";
  elements.generatorForm.elements.namedItem("serviceIntervalHours").value = "0";
  elements.generatorSubmit.textContent = "Add generator";
  elements.generatorCancel.hidden = true;
}

function resetVendorForm() {
  editingVendorId = null;
  elements.vendorForm.reset();
  elements.vendorSubmit.textContent = "Add vendor";
  elements.vendorCancel.hidden = true;
}

function resetInventoryForm() {
  editingInventoryId = null;
  elements.inventoryForm.reset();
  elements.inventoryForm.elements.namedItem("status").value = "In Stock";
  elements.inventoryForm.elements.namedItem("quantity").value = "1";
  elements.inventoryForm.elements.namedItem("minimumQuantity").value = "0";
  elements.inventorySubmit.textContent = "Add item";
  elements.inventoryCancel.hidden = true;
}

function resetExpenseForm() {
  editingExpenseId = null;
  elements.expenseForm.reset();
  seedFormDates();
  elements.expenseForm.elements.namedItem("currency").value = "USD";
  elements.expenseForm.elements.namedItem("status").value = "Planned";
  elements.expenseSubmit.textContent = "Add expense";
  elements.expenseCancel.hidden = true;
}

function resetInviteForm() {
  elements.inviteForm.reset();
  elements.inviteForm.elements.namedItem("role").value = "Crew";
}

function cloneDefaultState() {
  return JSON.parse(JSON.stringify(defaultState));
}

function cloneItems(items) {
  return JSON.parse(JSON.stringify(items));
}

function normalizePositiveInteger(value, fallback, min = 1) {
  const numericValue = Number(value);
  if (Number.isFinite(numericValue) && numericValue >= min) {
    return Math.round(numericValue);
  }

  const normalizedFallback = Number(fallback);
  if (Number.isFinite(normalizedFallback) && normalizedFallback >= min) {
    return Math.round(normalizedFallback);
  }

  return min;
}

function normalizeNumber(value, fallback) {
  const numericValue = Number(value);
  if (Number.isFinite(numericValue)) {
    return Math.round(numericValue * 10) / 10;
  }

  const normalizedFallback = Number(fallback);
  if (Number.isFinite(normalizedFallback)) {
    return Math.round(normalizedFallback * 10) / 10;
  }

  return 0;
}

function formatNumberValue(value) {
  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return "0";
  }

  return Number.isInteger(numericValue) ? String(numericValue) : numericValue.toFixed(1);
}

function clampPercent(value) {
  const numeric = Number(value);

  if (!Number.isFinite(numeric)) {
    return 0;
  }

  return Math.min(100, Math.max(0, Math.round(numeric)));
}

function vesselLevelStatus(type, value) {
  const safeValue = clampPercent(value);

  if (type === "grey") {
    if (safeValue >= 76) {
      return {
        label: "Pump out",
        toneClass: "tone-danger",
        fillClass: "is-danger",
      };
    }

    if (safeValue >= 46) {
      return {
        label: "Watch",
        toneClass: "tone-warn",
        fillClass: "is-warn",
      };
    }

    return {
      label: "Available",
      toneClass: "tone-good",
      fillClass: "is-good",
    };
  }

  if (safeValue <= 25) {
    return {
      label: "Low",
      toneClass: "tone-danger",
      fillClass: "is-danger",
    };
  }

  if (safeValue <= 50) {
    return {
      label: "Watch",
      toneClass: "tone-warn",
      fillClass: "is-warn",
    };
  }

  return {
    label: "Healthy",
    toneClass: "tone-good",
    fillClass: "is-good",
  };
}

function priorityClass(priority) {
  return `priority-${priority.toLowerCase()}`;
}

function statusClass(status) {
  return `status-${status.toLowerCase().replace(/\s+/g, "-")}`;
}

function categoryClass(category) {
  return `category-${category.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`;
}

function dayDifference(dateA, dateB) {
  const first = parseDateValue(dateA);
  const second = parseDateValue(dateB);
  first.setHours(0, 0, 0, 0);
  second.setHours(0, 0, 0, 0);
  return Math.round((second - first) / 86400000);
}

function isDateOverdue(value) {
  if (!value) {
    return false;
  }

  return dayDifference(value, todayStamp()) > 0;
}

function getMachineryHoursRemaining(item) {
  const interval = Number(item?.serviceIntervalHours || 0);
  if (!interval) {
    return null;
  }

  const lastServiceHours = Number(item?.lastServiceHours || 0);
  const currentHours = Number(item?.hours || 0);
  return Math.round((lastServiceHours + interval) - currentHours);
}

function getMachineryNextDueHours(item) {
  const interval = Number(item?.serviceIntervalHours || 0);
  if (!interval) {
    return null;
  }

  return Math.round(Number(item?.lastServiceHours || 0) + interval);
}

function getMachineryServiceState(item) {
  const hoursRemaining = getMachineryHoursRemaining(item);
  const daysUntilDateService = item?.nextServiceDate ? dayDifference(todayStamp(), item.nextServiceDate) : null;
  const hoursDue = hoursRemaining !== null && hoursRemaining <= 0;
  const hoursSoon = hoursRemaining !== null && hoursRemaining > 0 && hoursRemaining <= 25;
  const dateDue = daysUntilDateService !== null && daysUntilDateService < 0;
  const dateSoon = daysUntilDateService !== null && daysUntilDateService >= 0 && daysUntilDateService <= 14;

  if (hoursDue || dateDue) {
    return {
      label: "Due now",
      toneClass: "tone-danger",
      statusClass: "status-refit",
      fillClass: "is-danger",
      priority: "High",
    };
  }

  if (hoursSoon || dateSoon) {
    return {
      label: "Due soon",
      toneClass: "tone-warn",
      statusClass: "status-charter",
      fillClass: "is-warn",
      priority: "Medium",
    };
  }

  return {
    label: "On track",
    toneClass: "tone-good",
    statusClass: "status-ready",
    fillClass: "is-good",
    priority: "Low",
  };
}

function persistAndRender() {
  const snapshot = snapshotStateForPersistence();
  cacheLocalState(snapshot);
  if (authState.authenticated) {
    queueApiPersist(snapshot);
    void flushApiPersistQueue();
  }
  renderApp();
}
