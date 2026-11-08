/** Projects: the admin table and record panel (specs/007-ux-redesign T083 to T085, FR-007). */
import { RequireCapability } from "../../../src/capabilities";
import { ProjectsScreen } from "../../../src/screens/admin/AdminScreens";
import { NoAccessScreen } from "../../../src/screens/no-access/NoAccessScreen";

export default function Route() {
  return (
    <RequireCapability need="projects.manage" denied={<NoAccessScreen />}>
      <ProjectsScreen />
    </RequireCapability>
  );
}
