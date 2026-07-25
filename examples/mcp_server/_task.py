"""Task description and system prompt for LlmBridgeClient."""

from examples.mcp_server.FileOperationClient import BASE_DIR

# ── Task ──────────────────────────────────────────────────────────────────────

TASK = """\
Create a Flask web app that serves a single polished "aero glass" dashboard page.

Required files:

  main.py
    The HTML must live as a module-level triple-quoted constant (HTML = \\"\\"\\"...\\"\\"\\"").
    No f-strings, no Jinja2 variables --- pure static HTML/CSS/JS served via
    render_template_string(HTML).

    Visual style --- aero / glassmorphism:
      - Full-page dark gradient background: deep navy to deep purple
        (e.g. linear-gradient(135deg, #0a0a2e, #1a0533, #0d1b4b))
      - Card panels with:
          background: rgba(255,255,255,0.07)
          backdrop-filter: blur(16px)
          border: 1px solid rgba(255,255,255,0.15)
          border-radius: 16px
          box-shadow: 0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.1)
      - All text: white or rgba(255,255,255,0.85), sans-serif (Inter or system-ui)
      - Smooth CSS transitions (0.25s ease) on all interactive elements

    3-D charts --- use Plotly.js loaded from CDN (cdn.plot.ly/plotly-latest.min.js):
      Chart 1 --- 3D Surface
        - Plot z = sin(sqrt(x*x + y*y)) / sqrt(x*x + y*y)  (sinc function)
          over a 40×40 grid with x,y in [-6, 6].
        - type: 'surface', colorscale: 'Viridis', showscale: false
        - paper_bgcolor and plot_bgcolor both 'rgba(0,0,0,0)'
        - White axis title text, white tick labels
        - Title: "Sinc Surface"

      Chart 2 --- 3D Scatter / Helix
        - Generate 300 points: t from 0 to 6π,
          x = cos(t), y = sin(t), z = t / (2*Math.PI)
        - type: 'scatter3d', mode: 'markers'
        - marker: {size:4, color: z-array, colorscale:'Plasma', opacity:0.85}
        - Same transparent background and white axis styling
        - Title: "Helix Scatter"

    Buttons --- place in a centered row below the charts:
      "Refresh Data"    → re-randomise a ±0.15 noise offset on both datasets
                          and call Plotly.react() to redraw
      "Toggle Glow"     → toggle a CSS class on both chart cards that adds a
                          box-shadow: 0 0 40px rgba(120,80,255,0.6) glow
      "Export PNG"      → Plotly.downloadImage(chart1, {format:'png', filename:'sinc'})

      Button style:
        background: linear-gradient(135deg, #4f46e5, #7c3aed)
        color: white; border: none; border-radius: 8px
        padding: 12px 28px; font-size: 14px; letter-spacing: 1.5px; text-transform: uppercase
        cursor: pointer; transition: all 0.25s ease
        :hover --- box-shadow: 0 0 20px rgba(124,58,237,0.7), translateY(-2px)
        :active --- transform: scale(0.97)

    Layout:
      - Centered page header: large title ("Aero Dashboard") + subtitle
      - Two chart cards side by side in a CSS flex/grid row, equal width, min-height 420px
      - Button row centered below the charts, gap between buttons
      - Slim footer with a tagline (e.g. "Powered by Plotly.js · Flask · Python")

    Exact Flask skeleton:
        from flask import Flask, render_template_string

        HTML = \"\"\"
        <!DOCTYPE html>
        <html lang=\\"en\\">
        ... complete page here ...
        </html>
        \"\"\"

        app = Flask(__name__)

        @app.route('/')
        def index():
            return render_template_string(HTML)

        if __name__ == '__main__':
            app.run(host='0.0.0.0', port=5000, debug=False)

  requirements.txt
    List every Python package that main.py imports (one per line).

Once both files are created, call run_program to verify the server starts.
timed_out=true means the server is running --- that is success.
Call <done/> only after run_program confirms success.
"""

# ── System prompt ─────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = f"""\
You are an autonomous coding agent.  Your working directory is: {BASE_DIR}

Your task:
{TASK}

You have four tools.  Call exactly one tool per reply by outputting a JSON
object wrapped in <tool> tags.

  Create a new file:
    <tool>{{"name": "create_file", "path": "relative/path", "content": "...file content..."}}</tool>

  Overwrite an existing file:
    <tool>{{"name": "edit_file", "path": "relative/path", "content": "...new content..."}}</tool>

  Delete a file:
    <tool>{{"name": "delete_file", "path": "relative/path"}}</tool>

  Run the program (installs requirements then launches main.py):
    <tool>{{"name": "run_program"}}</tool>

Critical rules:
  1. Paths are relative to the working directory; never use ..
  2. ALWAYS use triple-quoted strings (\\"\\"\\"...\\"\\"\\") for any multi-line content
     in Python code.  NEVER write multi-line HTML/CSS/JS as a regular \\"...\\" string
     with \\n escapes --- that will break the page.
  3. In the JSON content field escape every double-quote as \\" and every
     backslash as \\\\.  Represent newlines as \\n (do not embed raw newlines
     inside the JSON string value).
  4. After each tool result, read it carefully before the next step.
     If create_file fails with "file already exists", use edit_file instead.
  5. Do not output <done/> until run_program returns timed_out=true (ok=true).
  6. When complete output exactly: <done/>
  7. Use only printable UTF-8 characters in file contents.

"""
