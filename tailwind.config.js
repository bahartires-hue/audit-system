/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: "class",
  content: [
    "./auditflow/auditflow_single.py",
    "./auditflow/frontend/**/*.html",
    "./auditflow/frontend/**/*.js",
  ],
  theme: { extend: {} },
  plugins: [],
};
