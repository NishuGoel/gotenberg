package libreoffice

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gotenberg/gotenberg/v7/pkg/gotenberg"
	"github.com/gotenberg/gotenberg/v7/pkg/modules/api"
	"github.com/gotenberg/gotenberg/v7/pkg/modules/libreoffice/uno"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// convertRoute returns an api.Route which can convert LibreOffice documents
// to PDF.
func convertRoute(unoAPI uno.API, engine gotenberg.PDFEngine) api.Route {
	return api.Route{
		Method:      http.MethodPost,
		Path:        "/forms/libreoffice/convert",
		IsMultipart: true,
		Handler: func(c echo.Context) error {
			ctx := c.Get("context").(*api.Context)

			// Let's get the data from the form and validate them.
			var (
				inputPaths         []string
				landscape          bool
				nativePageRanges   string
				nativePDFA1aFormat bool
				nativePDFformat    string
				PDFformat          string
				merge              bool
			)

			err := ctx.FormData().
				MandatoryPaths(unoAPI.Extensions(), &inputPaths).
				Bool("landscape", &landscape, false).
				String("nativePageRanges", &nativePageRanges, "").
				Bool("nativePdfA1aFormat", &nativePDFA1aFormat, false).
				String("nativePdfFormat", &nativePDFformat, "").
				String("pdfFormat", &PDFformat, "").
				Bool("merge", &merge, false).
				Validate()

			if err != nil {
				return fmt.Errorf("validate form data: %w", err)
			}

			if nativePDFA1aFormat {
				ctx.Log().Warn("'nativePdfA1aFormat' is deprecated; prefer 'nativePdfFormat' or 'pdfFormat' form fields instead")
			}

			if nativePDFA1aFormat && nativePDFformat != "" {
				return api.WrapError(
					errors.New("got both 'nativePdfFormat' and 'nativePdfA1aFormat' form fields"),
					api.NewSentinelHTTPError(http.StatusBadRequest, "Both 'nativePdfFormat' and 'nativePdfA1aFormat' form fields are provided"),
				)
			}

			if nativePDFA1aFormat && PDFformat != "" {
				return api.WrapError(
					errors.New("got both 'pdfFormat' and 'nativePdfA1aFormat' form fields"),
					api.NewSentinelHTTPError(http.StatusBadRequest, "Both 'pdfFormat' and 'nativePdfA1aFormat' form fields are provided"),
				)
			}

			if nativePDFformat != "" && PDFformat != "" {
				return api.WrapError(
					errors.New("got both 'pdfFormat' and 'nativePdfFormat' form fields"),
					api.NewSentinelHTTPError(http.StatusBadRequest, "Both 'pdfFormat' and 'nativePdfFormat' form fields are provided"),
				)
			}

			if nativePDFA1aFormat {
				nativePDFformat = gotenberg.FormatPDFA1a
			}

			// Alright, let's convert each document to PDF.

			outputPaths := make([]string, len(inputPaths))

			for i, inputPath := range inputPaths {
				outputPaths[i] = ctx.GeneratePath(".pdf")

				options := uno.Options{
					Landscape:  landscape,
					PageRanges: nativePageRanges,
					PDFformat:  nativePDFformat,
				}

				err = unoAPI.PDF(ctx, ctx.Log(), inputPath, outputPaths[i], options)

				if err != nil {
					if errors.Is(err, uno.ErrMalformedPageRanges) {
						return api.WrapError(
							fmt.Errorf("convert to PDF: %w", err),
							api.NewSentinelHTTPError(http.StatusBadRequest, fmt.Sprintf("Malformed page ranges '%s' (nativePageRanges)", options.PageRanges)),
						)
					}

					return fmt.Errorf("convert to PDF: %w", err)
				}
			}

			// So far so good, let's check if we have to merge the PDFs. Quick
			// win: if there is only one PDF, skip this step.

			if len(outputPaths) > 1 && merge {
				outputPath := ctx.GeneratePath(".pdf")

				err = engine.Merge(ctx, ctx.Log(), outputPaths, outputPath)
				if err != nil {
					return fmt.Errorf("merge PDFs: %w", err)
				}

				// Now, let's check if the client want to convert this result
				// PDF to a specific PDF format.

				// Note: nativePdfA1aFormat/nativePdfFormat have not been
				// specified if PDFformat is not empty.

				if PDFformat != "" {
					convertInputPath := outputPath
					convertOutputPath := ctx.GeneratePath(".pdf")

					err = engine.Convert(ctx, ctx.Log(), PDFformat, convertInputPath, convertOutputPath)

					if err != nil {
						if errors.Is(err, gotenberg.ErrPDFFormatNotAvailable) {
							return api.WrapError(
								fmt.Errorf("convert PDF: %w", err),
								api.NewSentinelHTTPError(
									http.StatusBadRequest,
									fmt.Sprintf("At least one PDF engine does not handle the PDF format '%s' (pdfFormat), while other have failed to convert for other reasons", PDFformat),
								),
							)
						}

						return fmt.Errorf("convert PDF: %w", err)
					}

					// Important: the output path is now the converted file.
					outputPath = convertOutputPath
				}

				// Last but not least, add the output path to the context so that
				// the API is able to send it as a response to the client.

				err = ctx.AddOutputPaths(outputPath)
				if err != nil {
					return fmt.Errorf("add output path: %w", err)
				}

				return nil
			}

			// Ok, we don't have to merge the PDFs. Let's check if the client
			// want to convert each PDF to a specific PDF format.

			// Note: nativePdfA1aFormat/nativePdfFormat have not been
			// specified if PDFformat is not empty.

			if PDFformat != "" {
				convertOutputPaths := make([]string, len(outputPaths))

				for i, outputPath := range outputPaths {
					convertInputPath := outputPath
					convertOutputPaths[i] = ctx.GeneratePath(".pdf")

					err = engine.Convert(ctx, ctx.Log(), PDFformat, convertInputPath, convertOutputPaths[i])

					if err != nil {
						if errors.Is(err, gotenberg.ErrPDFFormatNotAvailable) {
							return api.WrapError(
								fmt.Errorf("convert PDF: %w", err),
								api.NewSentinelHTTPError(
									http.StatusBadRequest,
									fmt.Sprintf("At least one PDF engine does not handle the PDF format '%s' (pdfFormat), while other have failed to convert for other reasons", PDFformat),
								),
							)
						}

						return fmt.Errorf("convert PDF: %w", err)
					}

				}

				// Important: the output paths are now the converted files.
				outputPaths = convertOutputPaths
			}

			// Last but not least, add the output paths to the context so that
			// the API is able to send them as a response to the client.

			err = ctx.AddOutputPaths(outputPaths...)
			if err != nil {
				return fmt.Errorf("add output paths: %w", err)
			}

			return nil
		},
	}
}

func createPNG(ctx context.Context, logger *zap.Logger, inputPath, outputPath string, options uno.Options) error {
	args := []string{
		"--no-launch",
		"--format",
		"png",
	}

	switch mod.libreOfficeRestartThreshold {
	case 0:
		listener := newLibreOfficeListener(logger, mod.libreOfficeBinPath, mod.libreOfficeStartTimeout, 0)

		err := listener.start(logger)
		if err != nil {
			return fmt.Errorf("start LibreOffice listener: %w", err)
		}

		defer func() {
			err := listener.stop(logger)
			if err != nil {
				logger.Error(fmt.Sprintf("stop LibreOffice listener: %v", err))
			}
		}()

		args = append(args, "--port", fmt.Sprintf("%d", listener.port()))
	default:
		err := mod.listener.lock(ctx, logger)
		if err != nil {
			return fmt.Errorf("lock long-running LibreOffice listener: %w", err)
		}

		defer func() {
			go func() {
				err := mod.listener.unlock(logger)
				if err != nil {
					mod.logger.Error(fmt.Sprintf("unlock long-running LibreOffice listener: %v", err))

					return
				}
			}()
		}()

		// If the LibreOffice listener is restarting while acquiring the lock,
		// the port will change. It's therefore important to add the port args
		// after we acquire the lock.
		args = append(args, "--port", fmt.Sprintf("%d", mod.listener.port()))
	}

	checkedEntry := logger.Check(zap.DebugLevel, "check for debug level before setting high verbosity")
	if checkedEntry != nil {
		args = append(args, "-vvv")
	}

	args = append(args, "--output", outputPath, inputPath)

	cmd, err := gotenberg.CommandContext(ctx, logger, mod.unoconvBinPath, args...)
	if err != nil {
		return fmt.Errorf("create unoconv command: %w", err)
	}

	logger.Debug(fmt.Sprintf("print to PNG with: %+v", options))

	activeInstancesCountMu.Lock()
	activeInstancesCount += 1
	activeInstancesCountMu.Unlock()

	exitCode, err := cmd.Exec()

	activeInstancesCountMu.Lock()
	activeInstancesCount -= 1
	activeInstancesCountMu.Unlock()

	if err == nil {
		return nil
	}

	// Unoconv/LibreOffice errors are not explicit.
	// That's why we have to make an educated guess according to the exit code
	// and given inputs.

	if exitCode == 5 && options.PageRanges != "" {
		return ErrMalformedPageRanges
	}

	// Possible errors:
	// 1. Unoconv/LibreOffice failed for some reason.
	// 2. Context done.
	//
	// On the second scenario, LibreOffice might not have time to remove some
	// of its temporary files, as it has been killed without warning. The
	// garbage collector will delete them for us (if the module is loaded).
	return fmt.Errorf("unoconv PNG: %w", err)
}

// generateThumnailRoute returns an api.Route which can generate image thumbnails for LibreOffice documents.
func generateThumnailRoute(unoAPI uno.API, engine gotenberg.PDFEngine) api.Route {
	return api.Route{
		Method:      http.MethodPost,
		Path:        "/forms/libreoffice/generate-thumbnail",
		IsMultipart: true,
		Handler: func(c echo.Context) error {
			ctx := c.Get("context").(*api.Context)

			// Let's get the data from the form and validate them.
			var (
				inputPaths         []string
				landscape          bool
				nativePageRanges   string
			)

			err := ctx.FormData().
				MandatoryPaths(unoAPI.Extensions(), &inputPaths).
				Bool("landscape", &landscape, false).
				String("nativePageRanges", &nativePageRanges, "").
				Validate()

			if err != nil {
				return fmt.Errorf("validate form data: %w", err)
			}

			if nativePDFA1aFormat {
				ctx.Log().Warn("'nativePdfA1aFormat' is deprecated; prefer 'nativePdfFormat' or 'pdfFormat' form fields instead")
			}

			if nativePDFA1aFormat && nativePDFformat != "" {
				return api.WrapError(
					errors.New("got both 'nativePdfFormat' and 'nativePdfA1aFormat' form fields"),
					api.NewSentinelHTTPError(http.StatusBadRequest, "Both 'nativePdfFormat' and 'nativePdfA1aFormat' form fields are provided"),
				)
			}

			if nativePDFA1aFormat && PDFformat != "" {
				return api.WrapError(
					errors.New("got both 'pdfFormat' and 'nativePdfA1aFormat' form fields"),
					api.NewSentinelHTTPError(http.StatusBadRequest, "Both 'pdfFormat' and 'nativePdfA1aFormat' form fields are provided"),
				)
			}

			if nativePDFformat != "" && PDFformat != "" {
				return api.WrapError(
					errors.New("got both 'pdfFormat' and 'nativePdfFormat' form fields"),
					api.NewSentinelHTTPError(http.StatusBadRequest, "Both 'pdfFormat' and 'nativePdfFormat' form fields are provided"),
				)
			}

			if nativePDFA1aFormat {
				nativePDFformat = gotenberg.FormatPDFA1a
			}

			// Alright, let's convert each document to PNG.

			outputPaths := make([]string, len(inputPaths))

			for i, inputPath := range inputPaths {
				outputPaths[i] = ctx.GeneratePath(".png")

				options := uno.Options{
					Landscape:  landscape,
					PageRanges: 1, // this gets the first page from a document for thumbnails
					PDFformat:  nativePDFformat,
				}

				err = createPNG(ctx, ctx.Log(), inputPath, outputPaths[i], options)

				if err != nil {
					if errors.Is(err, uno.ErrMalformedPageRanges) {
						return api.WrapError(
							fmt.Errorf("created thumbnail: %w", err),
							api.NewSentinelHTTPError(http.StatusBadRequest, fmt.Sprintf("Malformed page ranges '%s' (nativePageRanges)", options.PageRanges)),
						)
					}

					return fmt.Errorf("create thumbnail: %w", err)
				}
			}

			// Last but not least, add the output paths to the context so that
			// the API is able to send them as a response to the client.

			err = ctx.AddOutputPaths(outputPaths...)
			if err != nil {
				return fmt.Errorf("add output paths: %w", err)
			}

			return nil
		},
	}
}
