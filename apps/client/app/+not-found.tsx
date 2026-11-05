/** Any unknown address goes back to the start. */
import { Redirect } from "expo-router";

export default function NotFound() {
  return <Redirect href="/" />;
}
