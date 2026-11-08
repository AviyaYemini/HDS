/**
 * One shift (specs/007-ux-redesign FR-023): the site's address with links to the maps apps, the
 * post, the project's managers as the contacts, and "Add to calendar" (the device's calendar on a
 * phone, an .ics file on the web).
 */
// The import plugin cannot read expo-calendar's declarations; the names below are its documented API.
 
/* eslint-disable import/namespace -- the import plugin cannot read expo-calendar's declarations; the names used are its documented API and tsc checks them. */
import * as Calendar from "expo-calendar";
import { Link, useLocalSearchParams } from "expo-router";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Linking, Platform, StyleSheet, View } from "react-native";

import { call, useErrorText } from "../../../api/errors";
import { useSession } from "../../../api/SessionContext";
import { previewsEnabled } from "../../../config/previews";
import { Button, EmptyState, InlineBanner, PageTitle, SkeletonRows, StatusChip, Txt, showToast } from "../../../design/components";
import { Screen } from "../../../design/components/Screen";
import { Icon } from "../../../design/icons/Icon";
import { useTheme } from "../../../design/theme/ThemeProvider";
import { formatLongDate, formatTimeRange, isolateLtr } from "../../../locale";
import { ContactCard } from "../../client/ClientScreens";
import { useLoad } from "../../useLoad";
import { icsFor, mapLinks, shiftDate, type ShiftDetail } from "./model";

/** Hand the browser an .ics file to save (FR-023). */
function downloadIcs(text: string, name: string): void {
  if (typeof document === "undefined") return;
  const blob = new Blob([text], { type: "text/calendar;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = name;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** Put the shift in the phone's calendar; false when the person refused the permission. */
async function addToDeviceCalendar(shift: ShiftDetail, title: string): Promise<boolean> {
  const { status } = await Calendar.requestCalendarPermissionsAsync();
  if (status !== "granted") return false;
  let calendarId: string | undefined;
  if (Platform.OS === "ios") {
    calendarId = (await Calendar.getDefaultCalendarAsync()).id;
  } else {
    const calendars = await Calendar.getCalendarsAsync(Calendar.EntityTypes.EVENT);
    calendarId = (calendars.find((calendar) => calendar.allowsModifications) ?? calendars[0])?.id;
  }
  if (!calendarId) return false;
  await Calendar.createEventAsync(calendarId, {
    title,
    startDate: new Date(shift.starts_at),
    endDate: new Date(shift.ends_at),
    location: shift.site_address ?? shift.site.name,
    notes: `${shift.project.name} · ${shift.site.name}`,
    timeZone: shift.timezone,
  });
  return true;
}

export function ShiftDetailScreen() {
  const { t, i18n } = useTranslation();
  const { id } = useLocalSearchParams<{ id: string }>();
  const session = useSession();
  const errorText = useErrorText();
  const { colors } = useTheme();
  const [adding, setAdding] = useState(false);
  const shift = useLoad(() => call(session.api.GET("/me/shifts/{id}", { params: { path: { id: String(id) } } })), [id]);

  const addToCalendar = async (detail: ShiftDetail): Promise<void> => {
    const title = t("shifts.calendar_title", { type: detail.shift_type, site: detail.site.name });
    if (Platform.OS === "web") {
      downloadIcs(icsFor(detail, title), `hds-shift-${shiftDate(detail)}.ics`);
      return;
    }
    setAdding(true);
    try {
      const added = await addToDeviceCalendar(detail, title);
      showToast(added ? t("shifts.calendar_added") : t("shifts.calendar_refused"), added ? {} : { tone: "warning" });
    } catch {
      showToast(t("shifts.calendar_failed"), { tone: "warning" });
    } finally {
      setAdding(false);
    }
  };

  return (
    <Screen testID="screen-shift-detail">
      {shift.error ? (
        <InlineBanner tone="warning" title={errorText(shift.error)} action={{ label: t("common.retry"), onPress: () => void shift.reload() }} />
      ) : shift.data === null ? (
        <SkeletonRows rows={4} />
      ) : (
        <>
          <PageTitle eyebrow={[t("app.name"), t("shifts.title")]} title={formatLongDate(shiftDate(shift.data), i18n.language)}>
            <StatusChip status="neutral" label={shift.data.shift_type} />
          </PageTitle>
          <View style={[styles.facts, { borderColor: colors.border.subtle }]}>
            <Fact label={t("shifts.detail_time")} value={formatTimeRange(shift.data.starts_at, shift.data.ends_at, shift.data.timezone)} numeric />
            <Fact label={t("shifts.detail_project")} value={shift.data.project.name} />
            <Fact label={t("shifts.detail_site")} value={shift.data.site.name} />
            <Fact label={t("shifts.detail_address")} value={shift.data.site_address ?? t("shifts.no_address")} testID="shift-address" />
            {shift.data.site_address ? <MapLinks address={shift.data.site_address} /> : null}
          </View>
          <View style={styles.section}>
            <Txt role="section" heading={2}>
              {t("shifts.contacts")}
            </Txt>
            {shift.data.contacts.length === 0 ? (
              // No manager on the project: the company's own contact details, and the contact page (FR-007).
              <View style={styles.section} testID="shift-org-contact">
                <Txt role="body" tone="secondary">
                  {t("shifts.no_contacts")}
                </Txt>
                <ContactCard testID="shift-org-contact-card" />
                <Link href="/contact" asChild>
                  <Button variant="link" label={t("nav.contact")} testID="link-contact-page" />
                </Link>
              </View>
            ) : (
              shift.data.contacts.map((contact) => (
                <View key={contact.email} testID="shift-contact" style={[styles.contact, { borderColor: colors.border.subtle }]}>
                  <Icon name="user" size={18} color={colors.text.secondary} />
                  <View style={styles.contactBody}>
                    <Txt role="label">{contact.full_name}</Txt>
                    <Txt role="meta" tone="secondary">
                      {t("shifts.project_manager")}
                    </Txt>
                  </View>
                  {contact.phone ? <Button variant="link" label={isolateLtr(contact.phone)} icon="phone" onPress={() => void Linking.openURL(`tel:${contact.phone}`)} /> : null}
                  <Button variant="link" label={t("shifts.email")} icon="mail" onPress={() => void Linking.openURL(`mailto:${contact.email}`)} />
                </View>
              ))
            )}
          </View>
          <Button variant="primary" label={t("shifts.add_to_calendar")} icon="calendar" onPress={() => void addToCalendar(shift.data!)} busy={adding} testID="action-add-to-calendar" />
          {previewsEnabled ? (
            <EmptyState testID="preview-cant-make-it" icon="mail" badge={t("preview.badge")} title={t("preview.cant_make_it_title")} body={t("preview.cant_make_it_body")} feature={t("preview.comes_with", { feature: "011" })} />
          ) : null}
        </>
      )}
    </Screen>
  );
}

function Fact({ label, value, numeric, testID }: { label: string; value: string; numeric?: boolean; testID?: string }) {
  return (
    <View style={styles.fact}>
      <Txt role="meta" tone="secondary">
        {label}
      </Txt>
      <Txt role="body" numeric={numeric} testID={testID}>
        {value}
      </Txt>
    </View>
  );
}

function MapLinks({ address }: { address: string }) {
  const { t } = useTranslation();
  const links = mapLinks(address);
  return (
    <View style={styles.links} testID="shift-map-links">
      <Button variant="link" label={t("shifts.open_in_maps", { app: "Google Maps" })} onPress={() => void Linking.openURL(links.google)} />
      <Button variant="link" label={t("shifts.open_in_maps", { app: "Waze" })} onPress={() => void Linking.openURL(links.waze)} />
      {Platform.OS === "ios" ? <Button variant="link" label={t("shifts.open_in_maps", { app: "Apple Maps" })} onPress={() => void Linking.openURL(links.apple)} /> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  facts: { gap: 12, padding: 16, borderWidth: 1, maxWidth: 640 },
  fact: { gap: 2 },
  links: { flexDirection: "row", flexWrap: "wrap", gap: 12 },
  section: { gap: 10 },
  contact: { flexDirection: "row", alignItems: "center", flexWrap: "wrap", gap: 12, padding: 12, borderWidth: 1 },
  // The width is the basis, so the row wraps on a narrow screen (FR-079).
  contactBody: { flexGrow: 1, flexShrink: 1, flexBasis: 140, gap: 2 },
});
