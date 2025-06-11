library(shiny)
library(bslib)
library(tidyverse)
library(DBI)
library(RSQLite)
library(duckdb)
library(RMariaDB)
library(arrow)

# readRenviron(".Renviron") # needed for shinyapps.io

# setup -----------------------------------------------

has_column <- possibly(\(...) ncol(select(...)) > 0, otherwise = FALSE)

language_codes <- read_csv("data/iso_639_codes.csv") |>
  mutate(iso_code = paste0("\\b", iso_code, "\\b")) |>
  {
    \(x) setNames(x$language, x$iso_code)
  }()

get_data <- function(backend, .table = "metadata") {
  mysql_conn <- dbConnect(
    MariaDB(),
    dbname = Sys.getenv("MYSQL_DBNAME"),
    username = Sys.getenv("MYSQL_USER"),
    password = Sys.getenv("MYSQL_PASSWORD"),
    host = Sys.getenv("MYSQL_HOST"),
    mysql = TRUE
  )

  if (!backend %in% c("...", "Parquet")) {
    the_conn <- switch(
      backend,
      SQLite = dbConnect(SQLite(), dbname = "data/gutenberg.sqlite"),
      duckDB = dbConnect(duckdb(), dbdir = "data/gutenberg.duckdb"),
      mySQL = mysql_conn
    )

    time_start <- Sys.time()
    result <- dbReadTable(the_conn, .table)
    time_end <- Sys.time()

    data.frame(
      timestamp = Sys.time(),
      backend = backend,
      tbl_name = .table,
      read_time = time_end - time_start
    ) |>
      dbWriteTable(
        conn = mysql_conn,
        name = "performance",
        value = _,
        append = TRUE
      )

    dbDisconnect(mysql_conn)
    if (backend != "mySQL") {
      dbDisconnect(the_conn)
    }

    attributes(result)$elapsed <- time_end - time_start
  } else if (backend == "Parquet") {
    time_start <- Sys.time()
    result <- read_parquet(glue::glue("data/gutenberg_{.table}.parquet"))
    time_end <- Sys.time()

    data.frame(
      timestamp = Sys.time(),
      backend = backend,
      tbl_name = .table,
      read_time = time_end - time_start
    ) |>
      dbWriteTable(
        conn = mysql_conn,
        name = "performance",
        value = _,
        append = TRUE
      )

    dbDisconnect(mysql_conn)

    attributes(result)$elapsed <- time_end - time_start
  }
  result
}

format_bytes <- function(x){
  case_when(
    x > 1000000 ~ round(x / 1000000, 2) |> paste("MB"),
    x > 1000 ~ round(x / 1000, 2) |> paste("KB"),
    .default = x |> paste("bytes")
  )
}

files_parquet <- list.files("data", pattern = ".parquet", full.names = TRUE)
file_parquet <- list.files(
  "data",
  pattern = "metadata.parquet",
  full.names = TRUE
)
file_duckdb <- list.files("data", pattern = ".duckdb", full.names = TRUE)
file_sqlite <- list.files("data", pattern = ".sqlite", full.names = TRUE)

sqlite_temp <- tempfile(fileext = ".sqlite")
file.copy(file_sqlite, sqlite_temp)

block_query <- function(query, string){
  need(
    !str_detect(query, string),
    paste(string, "statements aren't supported. (Please be kind to the server.)")
  )
}

query_languages <- c(
  "SELECT language, COUNT(*) AS n FROM metadata
GROUP BY language
ORDER BY n DESC",
  "SELECT language, COUNT(*) FROM metadata
GROUP BY language
ORDER BY COUNT(*) DESC")

query_author_date <- c(
  "SELECT author, birthdate AS born FROM authors 
WHERE birthdate < 0
ORDER BY author",
  "SELECT author, birthdate, deathdate, deathdate - birthdate AS age FROM authors 
WHERE deathdate = 2000
ORDER BY birthdate
LIMIT 20",
  "SELECT * FROM authors 
WHERE birthdate IS 1882
AND deathdate IS 1941
LIMIT 10"
)

query_texts <- c(
  "SELECT * FROM metadata
WHERE author = 'Shakespeare, William'
AND language IS NOT 'en'
LIMIT 20",
  "SELECT * FROM metadata
WHERE author = 'Dickens, Charles'
AND has_text = 1
LIMIT 20",
  "SELECT * FROM metadata
WHERE author = 'Woolf, Virginia'
AND has_text = 1
LIMIT 20",
  "SELECT * FROM metadata
WHERE title = 'Othello'
AND author IS NOT 'Shakespeare, William'
LIMIT 20"
)

query_random <- c(
  query_author_date,
  query_texts,
  query_languages,
  "SELECT * FROM subjects LIMIT 15",
  "SELECT * FROM metadata
WHERE language = 'fr' 
  AND has_text=1 
LIMIT 10"
)

# UI --------------------------------------------------

ui <- page_navbar(
  theme = bs_theme(version = 5, bootswatch = "journal"),
  id = "the_page",
  title = "Movable Data Type",
  nav_spacer(),

  ## ui - Structure -------------------------------------

  nav_panel(
    title = "Structure",
    layout_sidebar(
      sidebar = sidebar(
        p(tags$i("From movable type to", tags$b("Movable Data Type"))), 
        p("Explore structure and movement across different backends using data from the Project Gutenberg corpus."),
        wellPanel(
          selectInput(
            "db_backend",
            "Backend:",
            selected = sample(c("Parquet", "duckDB", "SQLite"), size = 1),
            choices = c("Parquet", "duckDB", "SQLite", "mySQL")
          ),
          selectInput(
            "db_table",
            "Table:",
            choices = c("metadata", "authors", "languages", "subjects")
          )
        ),
        p(tags$i(
          "Each change of backend or table adds a row to a remote mySQL table, so UI updates may appear slower than the recorded read time."
        ))
      ),
      layout_columns(
        max_height = "210vh",
        col_widths = c(12, 8, 4),
        row_heights = c(1, 4, 3),
        card(
          card_header("Schema"),
          tableOutput("tbl_schema")
        ),
        layout_columns(
          max_height = "100vh",
          col_widths = 12,
          navset_card_tab(
            title = "Speed Comparison",
            nav_panel(
              title = "Chart",
              plotOutput("box_performance")
            ),
            nav_panel(
              title = "Table",
              tableOutput("tbl_performance")
            )
          )
        ),
        layout_columns(
          col_widths = 12,
          row_heights = 1,
          uiOutput("valuebox_speed"),
          uiOutput("valuebox_speed_per_row"),
          uiOutput("valuebox_size"),
          uiOutput("valuebox_rows"),
          uiOutput("valuebox_columns")
        )
      )
    )
  ),

  ## ui - Query -----------------------------------

  nav_panel(
    title = "Query",
    layout_sidebar(
      sidebar = sidebar(
        p("Practice querying tables using Structured Query Language (SQL) syntax."),
        tags$h5("Sample queries"),
        wellPanel(
          tags$ul(
            tags$li(actionLink("sql_list_tables", "All tables")),
            tags$li(actionLink("sql_languages", "Common languages")),
            tags$li(actionLink("sql_text", "Texts by metadata")),
            tags$li(actionLink("sql_author_date", "Authors by dates")),
            tags$li(actionLink("sql_random_query", "Surprise me")))),
        tags$h5("Tables and columns"),
        accordion(
          open = FALSE,
          accordion_panel(
            title = "metadata",
            tags$ul(map(
              c(
                "gutenberg_id",
                "title",
                "author",
                "gutenberg_author_id",
                "language",
                "gutenberg_bookshelf",
                "rights",
                "has_text"
              ),
              tags$li
            ))
          ),
          accordion_panel(
            title = "authors",
            tags$ul(map(
              c(
                "gutenberg_author_id",
                "author",
                "alias",
                "birthdate",
                "deathdate",
                "wikipedia"
              ),
              tags$li
            ))
          ),
          accordion_panel(
            title = "languages",
            tags$ul(map(
              c("gutenberg_id", "language", "total_languages"),
              tags$li
            ))
          ),
          accordion_panel(
            title = "languages",
            tags$ul(map(c("gutenberg_id", "subject_type", "subject"), tags$li))
          )
        ),
        p(tags$i(
          "Each query runs against a temporary copy of the SQLite data, isolated to this app instance. Experiment with SQL for all four tables without affecting the original database. A few commands are blocked to maintain responsiveness."
        ))
      ),
      card(
        fill = FALSE,
        card_header("SQLite Query"),
        card_body(textAreaInput(
          "sql_query",
          NULL,
          value = "SELECT * FROM metadata LIMIT 10",
          width = "100%",
          rows = 4,
        ),
        actionButton("submit_button", label = "Submit", width = "100%"))
      ),
      card(
        card_header("Result"),
        tableOutput("sql_result"))
    )
  ),

  ## ui - Languages -----------------------------------

  nav_panel(
    title = "Languages",
    layout_sidebar(
      sidebar = sidebar(
        p("Visualize language distributions in the Project Gutenberg dataset."),
        wellPanel(
          numericInput(
            "top_n", "Languages", 
            value = 5, min = 1, max = 7),
          selectInput(
            "lang_filter",
            "Exclude",
            multiple = TRUE,
            choices = c()),
          radioButtons(
            "chart_type",
            "Chart type",
            choices = c("Bar", "Pie", "Waffle"))
        ),
        p(tags$i("Charts reflect the backend and table selections set on the", shiny::actionLink("structure_link", "Structure page"), "."))
      ),
      card(plotOutput("plot_languages"))
    )
  ),
  nav_spacer(),
  nav_item(
    tags$a(
      href="https://github.com/jmclawson/movable-data-type",
      icon("github"))
  )
)

# server ----------------------------------------------

server <- function(input, output, session) {
  the_data <- reactive({
    get_data(input$db_backend, input$db_table)
  })

  the_languages <- reactive({
    validate(
      need(
        has_column(the_data(), language),
        "Please use the 'metadata' or 'language' table to study languages."
      )
    )
    the_data() |>
      mutate(language = language |> str_replace_all(language_codes))
  })

  db_performance <- reactive({
    mysql_conn <- dbConnect(
      MariaDB(),
      dbname = Sys.getenv("MYSQL_DBNAME"),
      username = Sys.getenv("MYSQL_USER"),
      password = Sys.getenv("MYSQL_PASSWORD"),
      host = Sys.getenv("MYSQL_HOST"),
      mysql = TRUE
    )
    
    on.exit(dbDisconnect(mysql_conn))
    dbReadTable(mysql_conn, "performance")
  }) |>
    bindEvent(input$db_backend, input$db_table)

  filtered_data <- reactive({
    the_languages() |>
      filter(!language %in% input$lang_filter)
  })
  
  ## server - Structure --------------------------------
  
  ### server - Structure - value boxes -----------------

  output$n_rows <- renderText({
    the_data() |>
      nrow() |>
      scales::label_comma()()
  })
  
  output$n_cols <- renderText({
    the_data() |>
      ncol()
  })
  
  output$time_per_row <- renderText({
    result <- (as.numeric(attributes(the_data())$elapsed) / nrow(the_data()))
    
    paste(round(result * 1000000, 4), "ms")
  })
  
  output$elapsed_time <- renderText({
    attributes(the_data())$elapsed |>
      round(4) |>
      paste("sec")
  })
  
  output$valuebox_speed <- renderUI({
    value_box(
      title = paste(input$db_backend, "read time"),
      value = textOutput("elapsed_time"),
      theme = case_when(
        attributes(the_data())$elapsed < 0.1 ~ "bg-gradient-green-teal",
        attributes(the_data())$elapsed < 1 ~ "bg-gradient-indigo-blue",
        attributes(the_data())$elapsed < 2 ~ "warning",
        TRUE ~ "danger"
      ),
      showcase = icon(
        case_when(
          attributes(the_data())$elapsed < 0.1 ~ "hourglass-start",
          attributes(the_data())$elapsed < 1 ~ "hourglass-half",
          TRUE ~ "hourglass-end"
        )
      )
    )
  })
  
  output$valuebox_speed_per_row <- renderUI({
    value_box(
      title = "Read time per row",
      showcase = icon("stopwatch"),
      value = textOutput("time_per_row"),
    )
  })
  
  output$valuebox_rows <- renderUI({
    value_box(
      title = "Rows",
      theme = "info",
      showcase = icon("diagram-next"),
      value = textOutput("n_rows"),
    )
  })
  
  output$valuebox_columns <- renderUI({
    value_box(
      title = "Columns",
      theme = "info",
      showcase = icon("table-columns"),
      value = textOutput("n_cols"),
    )
  })
  
  output$file_size <- renderText({
    if (input$db_backend != "mySQL") {
      the_file <- switch(
        input$db_backend,
        duckDB = file_duckdb,
        Parquet = file_parquet,
        SQLite = file_sqlite
      )
      
      file.size(the_file) |>
        format_bytes()
    } else {
      "31.1 MiB" # Mebibytes reported by server
    }
  })
  
  output$valuebox_size <- renderUI({
    req(input$db_backend)
    value_box(
      title = paste0(
        if_else(
          input$db_backend != "mySQL",
          "local ",
          "remote "
        ),
        input$db_backend,
        if_else(
          input$db_backend != "mySQL",
          " file",
          ""
        ),
        " size"
      ),
      value = textOutput("file_size"),
      showcase = icon("floppy-disk")
    )
  })
  
  ### server - Structure - tables ----------------------
  
  output$tbl_schema <- renderTable({
    if (input$db_backend != "Parquet") {
      the_table <- input$db_table
      the_conn <- switch(
        input$db_backend,
        SQLite = dbConnect(SQLite(), dbname = "data/gutenberg.sqlite"),
        duckDB = dbConnect(duckdb(), dbdir = "data/gutenberg.duckdb"),
        mySQL = dbConnect(
          MariaDB(),
          dbname = Sys.getenv("MYSQL_DBNAME"),
          username = Sys.getenv("MYSQL_USER"),
          password = Sys.getenv("MYSQL_PASSWORD"),
          host = Sys.getenv("MYSQL_HOST"),
          mysql = TRUE
        )
      )
      if (input$db_backend %in% c("duckDB", "SQLite")) {
        the_query <- glue::glue("PRAGMA table_info('{the_table}')")
      } else if (input$db_backend == "mySQL") {
        the_query <- glue::glue("SHOW COLUMNS FROM {the_table}")
      }
      on.exit(dbDisconnect(the_conn))
      dbGetQuery(the_conn, the_query)
    } else {
      parquet_schema <- files_parquet |>
        str_subset(input$db_table) |>
        open_dataset() |>
        schema()
      
      data.frame(
        column = names(parquet_schema),
        type = sapply(parquet_schema, \(x) x$type$ToString())
      )
    }
  })
  
  output$tbl_performance <- renderTable({
    db_performance() |>
      filter(tbl_name == input$db_table) |>
      arrange(desc(timestamp)) |>
      mutate(
        timestamp = timestamp |>
          as.POSIXct() |>
          as.character() 
      )
  })
  
  ### server - Structure - plot ----------------------
  
  output$box_performance <- renderPlot({
    db_performance() |>
      filter(tbl_name == input$db_table) |>
      mutate(
        backend = backend |>
          fct_reorder(read_time)
      ) |>
      ggplot(aes(x = backend, y = read_time, fill = backend)) +
      geom_boxplot(show.legend = FALSE) +
      theme_minimal() +
      scale_y_log10() +
      labs(
        x = NULL,
        y = "seconds"
      ) +
      theme(
        panel.grid.major.x = element_blank(),
        panel.grid.minor.y = element_blank(),
        text = element_text(size = 18)
      ) +
      scale_fill_brewer(palette = "Dark2")
  })
  
  ## server - Query -----------------------------------
  
  ### server - Query - links --------------------------
  
  observe({
    updateTextAreaInput(
      session,
      "sql_query",
      value = 'SELECT name FROM sqlite_master WHERE type IS "table"'
    )
  }) |> 
    bindEvent(input$sql_list_tables)
  
  observe({
    the_query <- sample(query_languages, 1)
    updateTextAreaInput(
      session,
      "sql_query",
      value = the_query
    )
  }) |> 
    bindEvent(input$sql_languages)
  
  observe({
    the_query <- sample(query_author_date, 1)
    updateTextAreaInput(
      session,
      "sql_query",
      value = the_query
    )
  }) |> 
    bindEvent(input$sql_author_date)
  
  observe({
    the_query <- sample(query_texts, 1)
    updateTextAreaInput(
      session,
      "sql_query",
      value = the_query
    )
  }) |> 
    bindEvent(input$sql_text)
  
  observe({
    the_query <- sample(query_random, 1)
    updateTextAreaInput(
      session,
      "sql_query",
      value = the_query
    )
  }) |> 
    bindEvent(input$sql_random_query)
  
  ### server - Query - table --------------------------

  output$sql_result <- renderTable({
    validate(
      block_query(input$sql_query, "BEGIN"),
      block_query(input$sql_query, "PRAGMA"),
      block_query(input$sql_query, "VACUUM"),
      block_query(input$sql_query, "ANALYZE"),
      block_query(input$sql_query, "ATTACH"),
      block_query(input$sql_query, "DETACH"),
    )
    
    sqlite_db <- dbConnect(SQLite(), dbname = sqlite_temp)
    on.exit(dbDisconnect(sqlite_db))
    dbGetQuery(sqlite_db, input$sql_query)
  }) |>
    bindEvent(input$submit_button)
  
  ## server - Languages -------------------------------
  
  ### server - Languages - widgets --------------------
  
  observe({
    updateSelectInput(
      session,
      "lang_filter",
      choices = the_languages() |>
        pull(language) |>
        fct_infreq() |>
        sort() |>
        unique()
    )
  }) |>
    bindEvent(input$db_table == "metadata")
  
  observe({
    nav_select("the_page", selected = "Structure")
  }) |> 
    bindEvent(input$structure_link)
  
  ### server - Languages - plot -----------------------
  
  output$plot_languages <- renderPlot({
    lang_data <- filtered_data() |>
      mutate(
        language = language |>
          fct_infreq() |>
          fct_lump_n(n = input$top_n) |>
          fct_rev()
      )
    if (input$chart_type == "Bar") {
      the_plot <- lang_data |>
        ggplot(aes(
          y = language,
          fill = language
        )) +
        geom_bar(show.legend = FALSE) +
        scale_x_continuous(
          labels = scales::label_comma(),
          expand = expansion(mult = c(0, 0.05))) +
        theme_minimal() +
        theme(panel.grid.major.y = element_blank()) +
        labs(
          x = "Texts",
          y = NULL
        )
    } else if (input$chart_type == "Pie") {
      the_plot <- lang_data |>
        count(language) |>
        ggplot(aes(fill = language, x = "chart", y = n)) +
        geom_col(color = "white") +
        geom_text(
          aes(label = paste0(language, "\n", scales::label_comma()(n))),
          position = position_stack(vjust = 0.5),
          color = "white",
          show.legend = FALSE
        ) +
        theme_void() +
        theme(legend.position = "none") +
        labs(fill = element_blank()) +
        theme(
          plot.title = element_text(hjust = 0.5),
          plot.subtitle = element_text(hjust = 0.5)
        ) +
        coord_polar(theta = "y")
    } else {
      the_plot <- lang_data |>
        count(language) |>
        mutate(prop = round(100 * (n / sum(n)))) |>
        ggplot(aes(fill = language, values = prop)) +
        waffle::geom_waffle(n_rows = 10, color = "white") +
        theme_void() +
        theme(legend.position = "top") +
        labs(fill = NULL)
    }
    
    the_plot <- the_plot +
      labs(title = "Language Distribution in Project Gutenberg Texts") +
      scale_fill_brewer(palette = "Dark2") +
      theme(text = element_text(size = 18))
    
    the_subtitle <- if_else(input$top_n > 1, glue::glue("Top {input$top_n} languages"), "")
    the_subtitle <- if_else(
      length(input$lang_filter) > 0, 
      paste(the_subtitle, 
            paste0(
              "excluding ",
              str_flatten_comma(input$lang_filter, last = ", and ")
            )),
      the_subtitle) |> 
      str_squish() |> 
      str_to_title() |> 
      str_replace_all(" And ", " and ")
    
    if (the_subtitle != "") {
      the_plot <- the_plot +
        labs(subtitle = the_subtitle)
    }
    
    the_plot
  })
}

thematic::thematic_shiny()
shinyApp(ui, server)
