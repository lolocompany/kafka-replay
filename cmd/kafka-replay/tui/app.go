package tui

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/lolocompany/kafka-replay/v2/pkg"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// inferLocaleCharset mirrors gdamore/tcell/v2 charset_unix getCharset:
// tcell picks an I/O encoding from LC_ALL → LC_CTYPE → LANG. US-ASCII (C/POSIX)
// or other narrow codesets make encodeRune fall back to '?' for box-drawing.
func inferLocaleCharset() string {
	locale := os.Getenv("LC_ALL")
	if locale == "" {
		locale = os.Getenv("LC_CTYPE")
	}
	if locale == "" {
		locale = os.Getenv("LANG")
	}
	if locale == "POSIX" || locale == "C" {
		return "US-ASCII"
	}
	if i := strings.IndexRune(locale, '@'); i >= 0 {
		locale = locale[:i]
	}
	if i := strings.IndexRune(locale, '.'); i >= 0 {
		return locale[i+1:]
	}
	return "UTF-8"
}

func charsetLooksUTF8(cs string) bool {
	c := strings.TrimSpace(strings.ToLower(cs))
	return c == "utf-8" || c == "utf8" || strings.HasPrefix(c, "utf-8")
}

// prepareTTYLocaleForTcell sets UTF-8 ctype/lang when the process would otherwise
// use a narrow charset (common when LANG=C but the terminal can display UTF-8).
// Opt out: KAFKA_REPLAY_TUI_PRESERVE_LOCALE=1. If LC_ALL forces a non-UTF-8
// codeset other than C/POSIX, we do not override it.
func prepareTTYLocaleForTcell() {
	if os.Getenv("KAFKA_REPLAY_TUI_PRESERVE_LOCALE") == "1" {
		return
	}
	if charsetLooksUTF8(inferLocaleCharset()) {
		return
	}
	if v := os.Getenv("LC_ALL"); v != "" {
		if v == "C" || v == "POSIX" {
			_ = os.Setenv("LC_ALL", "en_US.UTF-8")
		}
		return
	}
	_ = os.Setenv("LC_CTYPE", "en_US.UTF-8")
	if lang := os.Getenv("LANG"); lang == "" || lang == "C" || lang == "POSIX" {
		_ = os.Setenv("LANG", "en_US.UTF-8")
	}
}

// useLimitedDrawing is true when the terminal is likely to replace Unicode box
// drawing and symbols with '?'. Cursor/VS Code panels often set TERM_PROGRAM=vscode
// while still breaking those glyphs. Override: KAFKA_REPLAY_TUI_UNICODE=1 (force
// Unicode) or KAFKA_REPLAY_TUI_ASCII_BORDERS=1 (force ASCII).
func useLimitedDrawing() bool {
	if os.Getenv("KAFKA_REPLAY_TUI_UNICODE") == "1" {
		return false
	}
	if os.Getenv("KAFKA_REPLAY_TUI_ASCII_BORDERS") == "1" {
		return true
	}
	tp := strings.ToLower(os.Getenv("TERM_PROGRAM"))
	if strings.Contains(tp, "vscode") ||
		strings.Contains(tp, "cursor") ||
		strings.Contains(tp, "jetbrains") {
		return true
	}
	// VS Code / Cursor sometimes omit TERM_PROGRAM; these are set in the integrated shell.
	if os.Getenv("VSCODE_PID") != "" || os.Getenv("VSCODE_IPC_HOOK_CLI") != "" {
		return true
	}
	return false
}

// asciiBorders forces +/-/| borders for limited terminals.
func asciiBorders() {
	tview.Borders.Horizontal = '-'
	tview.Borders.Vertical = '|'
	tview.Borders.TopLeft = '+'
	tview.Borders.TopRight = '+'
	tview.Borders.BottomLeft = '+'
	tview.Borders.BottomRight = '+'
	tview.Borders.LeftT = '+'
	tview.Borders.RightT = '+'
	tview.Borders.TopT = '+'
	tview.Borders.BottomT = '+'
	tview.Borders.Cross = '+'
	tview.Borders.HorizontalFocus = '='
	tview.Borders.VerticalFocus = '|'
	tview.Borders.TopLeftFocus = '+'
	tview.Borders.TopRightFocus = '+'
	tview.Borders.BottomLeftFocus = '+'
	tview.Borders.BottomRightFocus = '+'
}

// applyK9sTheme matches the k9s look on a typical UTF-8 terminal: black field,
// cyan box borders, orange titles, white body text.
func applyK9sTheme(limitedDrawing bool) {
	if limitedDrawing {
		asciiBorders()
	}
	bg := tcell.ColorBlack
	tview.Styles = tview.Theme{
		PrimitiveBackgroundColor:    bg,
		ContrastBackgroundColor:     tcell.NewRGBColor(80, 140, 220),
		MoreContrastBackgroundColor: tcell.NewRGBColor(40, 90, 160),
		BorderColor:                 tcell.NewRGBColor(72, 200, 240),
		TitleColor:                  tcell.ColorOrange,
		GraphicsColor:               tcell.NewRGBColor(72, 200, 240),
		PrimaryTextColor:            tcell.ColorWhite,
		SecondaryTextColor:          tcell.NewRGBColor(180, 200, 220),
		TertiaryTextColor:           tcell.ColorAqua,
		InverseTextColor:            tcell.ColorBlack,
		ContrastSecondaryTextColor:  tcell.ColorWhite,
	}
}

// Run starts the tview application (k9s-style layout: header, crumb, body, hint bar).
func Run(ctx context.Context, cfg Config) error {
	prepareTTYLocaleForTcell()
	limited := useLimitedDrawing()
	applyK9sTheme(limited)

	dash, crumbMark, arrowHint, loading := "—", "▸", "↑↓", "Loading…"
	if limited {
		dash, crumbMark, arrowHint, loading = "-", ">", "Up/Down", "Loading..."
	}

	app := tview.NewApplication().EnableMouse(false)
	pages := tview.NewPages()

	currentPage := "menu"
	statusLines := []string{"Welcome to kafka-replay TUI."}

	appendStatus := func(line string) {
		statusLines = append(statusLines, line)
		if len(statusLines) > 100 {
			statusLines = statusLines[len(statusLines)-100:]
		}
	}

	header := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetText(fmt.Sprintf(" [orange::b]kafka-replay[white::-]  [grey]%s[white]  interactive mode", dash))

	crumb := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetText(fmt.Sprintf(" [aqua::b]%s HOME[white::-]", crumbMark))

	footer := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetText(" [grey]Enter[white] open  [grey]q[white] quit  [grey]Esc[white] back  [grey]Ctrl-C[white] quit")

	setFooter := func(text string) {
		footer.SetText(text)
	}

	selBar := tview.Styles.ContrastBackgroundColor
	selStyle := tcell.StyleDefault.Foreground(tcell.ColorBlack).Background(selBar)

	mainList := tview.NewList()
	mainList.ShowSecondaryText(true).SetHighlightFullLine(true)
	mainList.SetSelectedStyle(selStyle)
	mainList.SetBorder(true).SetTitle(" Main ")

	var goToMenu func()
	goToMenu = func() {
		currentPage = "menu"
		crumb.SetText(fmt.Sprintf(" [aqua::b]%s HOME[white::-]", crumbMark))
		setFooter(" [grey]Enter[white] open  [grey]t/r/y/s[white] jump  [grey]q[white] quit  [grey]Esc[white] back  [grey]Ctrl-C[white] quit")
		pages.SwitchToPage("menu")
		app.SetFocus(mainList)
	}

	statusView := tview.NewTextView()
	statusView.SetDynamicColors(true).SetWordWrap(true)
	statusView.SetBorder(true).SetTitle(" Status / log ")

	refreshStatus := func() {
		head := fmt.Sprintf("[aqua]Brokers:[white] %v\n\n", cfg.Brokers)
		statusView.SetText(head + strings.Join(statusLines, "\n"))
	}

	topicsTable := tview.NewTable()
	topicsTable.SetBorders(true).SetSelectable(true, false)
	topicsTable.SetSelectedStyle(selStyle)
	topicsTable.SetBorder(true).SetTitle(" Topics ")

	fillTopics := func(topics []pkg.TopicOutput) {
		topicsTable.Clear()
		h := func(text string) *tview.TableCell {
			return tview.NewTableCell(text).SetSelectable(false).SetAlign(tview.AlignLeft)
		}
		topicsTable.SetCell(0, 0, h("NAME"))
		topicsTable.SetCell(0, 1, h("PARTITIONS"))
		topicsTable.SetCell(0, 2, h("REPLICATION"))
		for i, t := range topics {
			r := i + 1
			topicsTable.SetCell(r, 0, tview.NewTableCell(t.Name))
			topicsTable.SetCell(r, 1, tview.NewTableCell(fmt.Sprintf("%d", t.PartitionCount)).SetAlign(tview.AlignRight))
			topicsTable.SetCell(r, 2, tview.NewTableCell(fmt.Sprintf("%d", t.ReplicationFactor)).SetAlign(tview.AlignRight))
		}
		if len(topics) > 0 {
			topicsTable.Select(1, 0)
		}
	}

	showTopicsError := func(err error) {
		topicsTable.Clear()
		topicsTable.SetCell(0, 0, tview.NewTableCell("Could not list topics").SetSelectable(false))
		topicsTable.SetCell(1, 0, tview.NewTableCell(err.Error()).SetSelectable(false))
	}

	openTopics := func() {
		currentPage = "topics"
		crumb.SetText(fmt.Sprintf(" [aqua::b]%s TOPICS[white::-]", crumbMark))
		setFooter(fmt.Sprintf(" [grey]%s[white] move  [grey]Enter[white] select row  [grey]Esc[white] home  [grey]r[white] refresh", arrowHint))
		topicsTable.Clear()
		topicsTable.SetCell(0, 0, tview.NewTableCell(loading).SetSelectable(false))
		pages.SwitchToPage("topics")
		app.SetFocus(topicsTable)

		go func() {
			topics, err := pkg.ListTopics(ctx, cfg.Brokers)
			app.QueueUpdateDraw(func() {
				if err != nil {
					showTopicsError(err)
					return
				}
				fillTopics(topics)
				appendStatus(fmt.Sprintf("Loaded %d topic(s).", len(topics)))
				refreshStatus()
			})
		}()
	}

	topicsTable.SetSelectedFunc(func(row, _ int) {
		if row < 1 {
			return
		}
		name := topicsTable.GetCell(row, 0).Text
		appendStatus("Selected topic: " + name)
		refreshStatus()
	})

	topicsTable.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		if ev.Rune() == 'r' || ev.Rune() == 'R' {
			openTopics()
			return nil
		}
		return ev
	})

	var topicRec, fileRec, topicRep, fileRep string

	recordForm := tview.NewForm()
	recordForm.AddInputField("Topic", "", 48, nil, func(t string) { topicRec = t })
	recordForm.AddInputField("Output file", "", 48, nil, func(t string) { fileRec = t })
	recordForm.AddButton("Submit (stub)", func() {
		appendStatus(fmt.Sprintf("Record requested: topic=%s file=%s", topicRec, fileRec))
		refreshStatus()
		goToMenu()
	})
	recordForm.AddButton("Cancel", goToMenu)
	recordForm.SetBorder(true).SetTitle(" Record ")

	replayForm := tview.NewForm()
	replayForm.AddInputField("Topic", "", 48, nil, func(t string) { topicRep = t })
	replayForm.AddInputField("Input file", "", 48, nil, func(t string) { fileRep = t })
	replayForm.AddButton("Submit (stub)", func() {
		appendStatus(fmt.Sprintf("Replay requested: topic=%s file=%s", topicRep, fileRep))
		refreshStatus()
		goToMenu()
	})
	replayForm.AddButton("Cancel", goToMenu)
	replayForm.SetBorder(true).SetTitle(" Replay ")

	openRecord := func() {
		currentPage = "record"
		crumb.SetText(fmt.Sprintf(" [aqua::b]%s RECORD[white::-]", crumbMark))
		setFooter(" [grey]Tab[white] fields  [grey]Enter[white] activate  [grey]Esc[white] home")
		pages.SwitchToPage("record")
		app.SetFocus(recordForm)
	}

	openReplay := func() {
		currentPage = "replay"
		crumb.SetText(fmt.Sprintf(" [aqua::b]%s REPLAY[white::-]", crumbMark))
		setFooter(" [grey]Tab[white] fields  [grey]Enter[white] activate  [grey]Esc[white] home")
		pages.SwitchToPage("replay")
		app.SetFocus(replayForm)
	}

	openStatus := func() {
		currentPage = "status"
		crumb.SetText(fmt.Sprintf(" [aqua::b]%s STATUS[white::-]", crumbMark))
		setFooter(" [grey]Esc[white] home")
		refreshStatus()
		pages.SwitchToPage("status")
		app.SetFocus(statusView)
	}

	mainList.AddItem("List Topics", "Browse topics (name, partitions, replication)", 't', openTopics)
	mainList.AddItem("Record", "Record messages from a topic to a file (stub)", 'r', openRecord)
	mainList.AddItem("Replay", "Replay messages from a file to a topic (stub)", 'y', openReplay)
	mainList.AddItem("Status / log", "Brokers and recent activity", 's', openStatus)

	pages.AddPage("menu", mainList, true, true)
	pages.AddPage("topics", topicsTable, true, false)
	pages.AddPage("record", recordForm, true, false)
	pages.AddPage("replay", replayForm, true, false)
	pages.AddPage("status", statusView, true, false)

	body := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(pages, 0, 1, true)

	shell := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(header, 1, 0, false).
		AddItem(crumb, 1, 0, false).
		AddItem(body, 0, 1, true).
		AddItem(footer, 1, 0, false)

	app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		switch ev.Key() {
		case tcell.KeyCtrlC:
			app.Stop()
			return nil
		case tcell.KeyEsc:
			if currentPage != "menu" {
				goToMenu()
				return nil
			}
		}
		if ev.Rune() == 'q' || ev.Rune() == 'Q' {
			if currentPage == "menu" {
				app.Stop()
				return nil
			}
		}
		return ev
	})

	go func() {
		<-ctx.Done()
		app.Stop()
	}()

	refreshStatus()
	return app.SetRoot(shell, true).SetFocus(mainList).Run()
}
