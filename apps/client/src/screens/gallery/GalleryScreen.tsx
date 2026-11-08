/**
 * The development gallery (specs/007-ux-redesign FR-067, task T020): every building block in every
 * state, in the current theme and language, so `tests/e2e/ux/a-design-system.spec.ts` and a person
 * can check them without data. The test ids it carries are the contract listed at the top of that
 * spec. Shown only while previews are on.
 */
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { StyleSheet, View } from "react-native";

import { API_URL } from "../../api/default";
import {
  Button,
  DateField,
  DateRangeField,
  EmptyState,
  FeaturePanel,
  InlineBanner,
  PageTitle,
  SearchablePicker,
  SegmentedControl,
  Sheet,
  Skeleton,
  SkeletonRows,
  StatusChip,
  Table,
  TextField,
  Txt,
  showToast,
  type ChipStatus,
  type PickerItem,
} from "../../design/components";
import { Screen } from "../../design/components/Screen";
import { Icon } from "../../design/icons/Icon";
import { track } from "../../design/loading/loadingStore";
import { THEME_NAMES, type ThemePreference } from "../../design/theme/themeStore";
import { useTheme } from "../../design/theme/ThemeProvider";
import { DEFAULT_TIME_ZONE, formatTimeRange, presetRange, todayIn, type DateRange } from "../../locale";

/** A sample shift, 06:00 to 14:00 in Jerusalem, for the direction check. */
const SAMPLE_SHIFT = { start: "2026-11-07T04:00:00Z", end: "2026-11-07T12:00:00Z" };

const CHIPS: ChipStatus[] = ["neutral", "ready", "warning", "success", "danger", "info"];
const THEME_KEYS: Record<ThemePreference, string> = {
  system: "theme.system",
  light: "theme.light",
  dark: "theme.dark",
  highContrastLight: "theme.high_contrast_light",
  highContrastDark: "theme.high_contrast_dark",
};

interface StaffRow {
  id: string;
  name: string;
  site: string;
  hours: string;
  status: ChipStatus;
}

function Section({ title, children, testID }: { title: string; children: React.ReactNode; testID?: string }) {
  const { colors } = useTheme();
  return (
    <View testID={testID} style={[styles.section, { borderTopColor: colors.border.strong }]}>
      <Txt role="section" heading={2}>
        {title}
      </Txt>
      {children}
    </View>
  );
}

export interface GalleryScreenProps {
  /** Today in the site's zone; the clock's by default, fixed by the unit tests. */
  today?: string;
}

/** The gallery. */
export function GalleryScreen({ today: givenToday }: GalleryScreenProps = {}) {
  const { t, i18n } = useTranslation();
  const theme = useTheme();
  const today = givenToday ?? todayIn(DEFAULT_TIME_ZONE);
  const [name, setName] = useState("");
  const [view, setView] = useState<"hours" | "pay" | "both">("hours");
  const [sheet, setSheet] = useState(false);
  const [date, setDate] = useState<string | null>(null);
  const [range, setRange] = useState<DateRange>(() => presetRange("this-week", today));
  const [picked, setPicked] = useState<string | null>(null);
  const [sort, setSort] = useState<{ key: string; direction: "asc" | "desc" } | null>({ key: "name", direction: "asc" });
  const [loaded, setLoaded] = useState(0);
  const [banner, setBanner] = useState(true);
  const sampleTime = formatTimeRange(SAMPLE_SHIFT.start, SAMPLE_SHIFT.end, DEFAULT_TIME_ZONE);

  const people: PickerItem[] = [
    { id: "p1", label: "Dana Levi", secondary: "Gate 3", keywords: ["דנה לוי"] },
    { id: "p2", label: "דנה לוי", secondary: "Night watch", keywords: ["Dana Levi"] },
    { id: "p3", label: "Yosef Cohen", secondary: "Reception", keywords: ["יוסף כהן"] },
    { id: "p4", label: "נועם ברק", secondary: "Gate 1", keywords: ["Noam Barak"] },
  ];
  const rows: StaffRow[] = [
    { id: "r1", name: "Dana Levi", site: "Gate 3", hours: "38.5", status: "success" },
    { id: "r2", name: "יוסף כהן", site: "Reception", hours: "42", status: "warning" },
    { id: "r3", name: "Noam Barak", site: "Gate 1", hours: "16", status: "neutral" },
  ];
  const sortedRows = [...rows].sort((a, b) => {
    if (!sort) return 0;
    const key = sort.key as keyof StaffRow;
    const order = String(a[key]).localeCompare(String(b[key]), i18n.language, { numeric: true });
    return sort.direction === "asc" ? order : -order;
  });

  const load = (): void => {
    void track(fetch(`${API_URL}/health`).then(() => undefined).catch(() => undefined)).then(() => setLoaded((n) => n + 1));
  };

  return (
    <Screen testID="gallery-root">
      <PageTitle eyebrow={t("gallery.eyebrow").split(" / ")} title={t("gallery.title")} testID="gallery-title">
        <SegmentedControl
          label={t("theme.label")}
          options={(["system", ...THEME_NAMES] as ThemePreference[]).map((value) => ({ value, label: t(THEME_KEYS[value]) }))}
          value={theme.preference}
          onChange={theme.setPreference}
          testID="gallery-theme"
        />
      </PageTitle>
      <Txt role="body" tone="secondary">
        {t("gallery.intro")}
      </Txt>

      <Section title={t("gallery.section.buttons")}>
        <View style={styles.row}>
          <Button variant="primary" label={t("gallery.cta")} onPress={() => undefined} testID="gallery-cta" />
          <Button variant="secondary" label={t("gallery.secondary")} onPress={() => undefined} />
          <Button variant="quiet" label={t("gallery.quiet")} icon="undo" onPress={() => undefined} />
          <Button variant="link" label={t("gallery.link")} onPress={() => undefined} testID="gallery-link" />
        </View>
        <View style={styles.row}>
          <Button variant="primary" label={t("gallery.disabled")} disabled disabledReason={t("gallery.disabled_reason")} />
          <Button variant="secondary" label={t("gallery.busy")} busy />
          <Button variant="secondary" label={t("gallery.secondary")} icon="check" iconPosition="trailing" onPress={() => undefined} />
        </View>
      </Section>

      <Section title={t("gallery.section.fields")}>
        <View style={styles.fields}>
          <TextField label={t("gallery.field.name")} value={name} onChangeText={setName} helper={t("gallery.field.helper")} testID="gallery-name" containerStyle={styles.field} />
          <TextField label={t("gallery.field.email")} value="dana@hds.test" onChangeText={() => undefined} direction="ltr" keyboardType="email-address" containerStyle={styles.field} />
          <TextField label={t("gallery.field.name")} value="" onChangeText={() => undefined} error={t("gallery.field.error")} containerStyle={styles.field} />
          <TextField label={t("gallery.field.readonly")} value="Gate 3" onChangeText={() => undefined} readOnly containerStyle={styles.field} />
          <TextField label={t("gallery.field.name")} value="Dana Levi" onChangeText={() => undefined} disabled disabledReason={t("gallery.disabled_reason")} containerStyle={styles.field} />
        </View>
        <SegmentedControl
          label={t("gallery.segment.label")}
          options={[
            { value: "hours", label: t("gallery.segment.hours"), testID: "gallery-segment-hours" },
            { value: "pay", label: t("gallery.segment.pay"), testID: "gallery-segment-pay" },
            { value: "both", label: t("gallery.segment.both"), testID: "gallery-segment-both" },
          ]}
          value={view}
          onChange={setView}
          testID="gallery-segment"
        />
        <SearchablePicker
          label={t("gallery.picker.label")}
          placeholder={t("gallery.picker.placeholder")}
          items={people}
          value={picked}
          onChange={setPicked}
          recentIds={["p3"]}
          testID="gallery-picker"
        />
      </Section>

      <Section title={t("gallery.section.chips")}>
        <View style={styles.row}>
          {CHIPS.map((status) => (
            <StatusChip key={status} status={status} label={t(`gallery.chip.${status}`)} testID={`gallery-chip-${status}`} />
          ))}
        </View>
      </Section>

      <Section title={t("gallery.section.panel")}>
        <FeaturePanel testID="gallery-panel">
          <Txt role="eyebrow" tone="secondary">
            {t("gallery.panel.eyebrow")}
          </Txt>
          <Txt role="section" heading={3}>
            {t("gallery.panel.title")}
          </Txt>
          <Txt role="body" tone="secondary">
            {t("gallery.panel.body")}
          </Txt>
        </FeaturePanel>
      </Section>

      <Section title={t("gallery.section.direction")}>
        <View style={styles.row} testID="gallery-row">
          <View style={[styles.cell, { backgroundColor: theme.colors.bg.selected }]} testID="gallery-row-leading">
            <Txt role="label">{t("gallery.row.first")}</Txt>
          </View>
          <View style={[styles.cell, { backgroundColor: theme.colors.bg.selected }]}>
            <Txt role="label">{t("gallery.row.middle")}</Txt>
          </View>
          <View style={[styles.cell, { backgroundColor: theme.colors.bg.selected }]} testID="gallery-row-trailing">
            <Txt role="label">{t("gallery.row.last")}</Txt>
          </View>
        </View>
        <View style={styles.row}>
          <Icon name="arrow-back" label={t("common.back")} testID="gallery-arrow-back" />
          <Icon name="arrow-next" label={t("common.next")} testID="gallery-arrow-next" />
          <Icon name="clock" label={t("gallery.time_label")} testID="gallery-clock-icon" />
          <Txt role="numeric" testID="gallery-time">
            {sampleTime}
          </Txt>
        </View>
      </Section>

      <Section title={t("gallery.section.table")}>
        <Table<StaffRow>
          caption={t("gallery.table.caption")}
          columns={[
            { key: "name", title: t("gallery.table.name"), sortable: true },
            { key: "site", title: t("gallery.table.site"), sortable: true },
            { key: "hours", title: t("gallery.table.hours"), numeric: true, sortable: true, width: 96 },
            { key: "status", title: t("gallery.table.status"), width: 140, render: (row) => <StatusChip status={row.status} label={t(`gallery.chip.${row.status}`)} /> },
          ]}
          rows={sortedRows}
          keyOf={(row) => row.id}
          sort={sort}
          onSort={(key, direction) => setSort({ key, direction })}
          onRowPress={() => undefined}
          selectedKey="r1"
          testID="gallery-table"
        />
      </Section>

      <Section title={t("gallery.section.dates")}>
        <View style={styles.fields}>
          <DateField label={t("gallery.date.single")} value={date} onChange={setDate} today={today} testID="date-field" style={styles.field} />
        </View>
        <DateRangeField label={t("gallery.date.range")} value={range} onChange={setRange} today={today} testID="date-range" />
      </Section>

      <Section title={t("gallery.section.feedback")}>
        {banner ? (
          <InlineBanner
            tone="warning"
            title={t("gallery.banner.title")}
            action={{ label: t("gallery.banner.action"), onPress: () => undefined }}
            onDismiss={() => setBanner(false)}
            testID="gallery-banner"
          >
            {t("gallery.banner.body")}
          </InlineBanner>
        ) : null}
        <InlineBanner tone="info">{t("gallery.banner.body")}</InlineBanner>
        <View style={styles.row}>
          <Button variant="secondary" label={t("gallery.show_toast")} onPress={() => showToast(t("gallery.toast"))} testID="gallery-toast" />
        </View>
        <EmptyState
          title={t("gallery.empty.title")}
          body={t("gallery.empty.body")}
          icon="calendar"
          action={{ label: t("gallery.empty.action"), onPress: () => undefined }}
          testID="gallery-empty"
        />
      </Section>

      <Section title={t("gallery.section.surfaces")}>
        <View style={styles.row}>
          <Button variant="secondary" label={t("gallery.open_sheet")} onPress={() => setSheet(true)} testID="gallery-open-sheet" />
        </View>
        <Sheet open={sheet} onClose={() => setSheet(false)} title={t("gallery.sheet.title")} testID="gallery-sheet">
          <Txt role="body">{t("gallery.sheet.body")}</Txt>
          <Button variant="primary" label={t("gallery.secondary")} onPress={() => setSheet(false)} />
        </Sheet>
      </Section>

      <Section title={t("gallery.section.loading")}>
        <View style={styles.row}>
          <Button variant="secondary" label={t("gallery.load")} onPress={load} testID="gallery-load" />
          {loaded > 0 ? (
            <Txt role="meta" tone="secondary" testID="gallery-load-count">
              {`${t("gallery.load_done")} ${loaded}`}
            </Txt>
          ) : null}
        </View>
        <Txt role="meta" tone="secondary">
          {t("gallery.skeleton")}
        </Txt>
        <View style={styles.row}>
          <Skeleton width={160} height={16} />
          <Skeleton width={96} height={16} />
        </View>
        <SkeletonRows rows={3} />
      </Section>
    </Screen>
  );
}

const styles = StyleSheet.create({
  section: { gap: 16, borderTopWidth: 1, paddingTop: 24 },
  row: { flexDirection: "row", flexWrap: "wrap", gap: 12, alignItems: "center" },
  fields: { flexDirection: "row", flexWrap: "wrap", gap: 16 },
  field: { flexBasis: 280, flexGrow: 1 },
  cell: { paddingHorizontal: 12, paddingVertical: 8 },
});
