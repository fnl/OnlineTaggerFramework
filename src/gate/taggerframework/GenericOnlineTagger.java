package gate.taggerframework;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Factory;
import gate.FeatureMap;
import gate.Gate;
import gate.ProcessingResource;
import gate.Resource;
import gate.Utils;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.creole.Transducer;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.creole.metadata.RunTime;
import gate.util.Files;
import gate.util.InvalidOffsetException;
import gate.util.OffsetComparator;
import gate.util.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This processing resource is designed to allow the easy use of a
 * external (command-line) taggers within GATE. A number of
 * assumptions have been made about the external taggers in order to
 * provide this framework, but any tagger that expects one annotation
 * type per line and outputs one annotation type per line (input and
 * output do not have to be the same) should be compatible with this PR.
 * This "online" version is updated to directly pipe the input to a
 * warm tagger process, instead of launching a new process for each
 * input annotation and writing both input and output files to for each
 * such call to disk, as the original GenericTagger does.
 * 
 * @author Mark A. Greenwood - original GenericTagger
 * @author Rene Witte - original GenericTagger
 * @author Florian Leitner - refactoring to online version
 */
@CreoleResource(comment = "online version of GATE's GenericTagger", 
    helpURL = "http://gate.ac.uk/userguide/sec:parsers:taggerframework")
public class GenericOnlineTagger extends AbstractLanguageAnalyser
	                               implements ProcessingResource {

	/* Note that the code here has been changed to make this class a bit more
	 * managable; Nonetheless, this is not clean code. From an object-oriented
	 * design perspective, there are a ton of pattern violations occurring
	 * here that have not been fixed. This implementation is only sufficient
	 * to provide a version that at least is orders of magnitude faster than
	 * the rather simple GenericTagger implementation. */

  private static final long serialVersionUID = 1065568768234638007L;

  // Name of the feature on the input annotations that will be sent to the tagger
  // Name of the feature on the output annotations that will be compared against the input
  private String stringFeatureName = "string";

	// Subprocess resources
	private OutputStream input;
	private InputStream output;
	private Process proc;

  // Optional transducers for pre- and post-processing
  private Transducer preProcess, postProcess;

  // The URLs of the JAPE grammars for pre- and post-processing
  private URL preProcessURL, postProcessURL;

  // The annotations sets used for input and output
  private String inputASName, outputASName;

  // The character encoding the tagger expects
  private String encoding;
  private Charset charset;

  // The regular expression that will process the output
  private String regex;
  private Pattern pattern;

  // The full path to the tagger binary
  // The directory to use when running the tagger
  private URL taggerBinary, taggerDir;

  // Command-line flags to pass to the tagger
  private List<String> taggerFlags;

  // Runtime flags:
  // display debug information
  // update or create output annotations
  // fail if an character (re-) encoding operation throws
  // fail if no input anntoations are present
  private Boolean debug, updateAnnotations, failOnUnmappableCharacter, failOnMissingInputAnnotations;

  // The type of the input and output annotations
  private String inputAnnotationType, outputAnnotationType;

  // Feature names mapped to regex capture group integers
  private FeatureMap featureMapping;

  protected Logger logger = Logger.getLogger(this.getClass().getName());

  /**
   * This method initialises the tagger. This involves loading the pre
   * and post processing JAPE grammars as well as a few sanity checks.
   * Most importantly, the tagger sub-process is launched here.
   * 
   * @throws ResourceInstantiationException if an error occurs while
   *           initialising the PR
   */
  @Override
  public Resource init() throws ResourceInstantiationException {
    // Create a feature map that we can use to load the two JAPE
    // transducers making sure they are hidden (i.e. don't appear in the
    // list of loaded PRs)
    FeatureMap hidden = Factory.newFeatureMap();
    Gate.setHiddenAttribute(hidden, true);
    FeatureMap params = Factory.newFeatureMap();

    if(preProcessURL != null) {
      params.put("grammarURL", preProcessURL);

      if(preProcess == null) {
        preProcess = (Transducer)Factory.createResource(
                "gate.creole.Transducer", params, hidden);
      }
      else {
        preProcess.setParameterValues(params);
        preProcess.reInit();
      }
    }

    if(postProcessURL != null) {
      params.put("grammarURL", postProcessURL);

      if(postProcess == null) {
        postProcess = (Transducer)Factory.createResource(
                "gate.creole.Transducer", params, hidden);
      }
      else {
        postProcess.setParameterValues(params);
        postProcess.reInit();
      }
    }

    // set up and start the tagger processes
    if (taggerBinary == null)
    	throw new ResourceInstantiationException(
    		"Cannot proceed unless a tagger executable is specified."
      );
    
    List<String> taggerCmd = buildCommandLine();
		proc = startTagger(taggerCmd);
		input = proc.getOutputStream();
		output = proc.getInputStream();

    return this;
  }

  /** Clean up associated resources, handles, and sub-processes. */
  public void cleanup() {
    if(preProcess != null) Factory.deleteResource(preProcess);
    if(postProcess != null) Factory.deleteResource(postProcess);

    proc.destroy();

    try {
			input.close();
			if (debug) System.err.println("Tagger shutdown complete");
		} catch (IOException e) {}
  }

  /**
   * This method constructs a List of Strings which will be used as
   * the command line for executing the external tagger through a 
   * process builder. This uses the tagger binary and flags to build
   * the command line. If the system property <code>shell.path</code>
   * has been set then the command line will be built so that the tagger
   * is run by the provided shell. This is useful on Windows where you
   * will usually need to run the tagger under Cygwin or the Command
   * Prompt.
   * 
   * @return the correctly assembled command line
   * @throws ExecutionException if any error occurs
   */
  protected List<String> buildCommandLine() throws ResourceInstantiationException {
		List<String> cmd = new LinkedList<String>();
    File taggerPath = Files.fileFromURL(taggerBinary);
    String shPath = System.getProperty("shell.path");
		StringBuilder sanityCheck = new StringBuilder();

    if (taggerPath.exists() == false) throw new ResourceInstantiationException(
    		"no binary found at " + taggerPath.getAbsolutePath());

    if (shPath != null) cmd.add(shPath);

    cmd.add(taggerPath.getAbsolutePath());
    cmd.addAll(taggerFlags);

		for (String s : cmd) sanityCheck.append(" ").append(s);

		logger.info("Tagger command: " + sanityCheck.toString());

    return cmd;
  }

	/** Start the tagger sub-process. */
  private Process startTagger(List<String> cmdline) throws ResourceInstantiationException {
    try {
    	ProcessBuilder builder = new ProcessBuilder(cmdline);

    	if (taggerDir != null) {
				File d;

				try {
						d = new File(taggerDir.toURI());
				} catch (URISyntaxException e) {
						d = new File(taggerDir.getPath());
				}

				builder.directory(d);
			}

    	builder.redirectErrorStream(true);
    	return builder.start();
    } catch (Exception e) {
      throw new ResourceInstantiationException(e);
    }
  }

  /**
   * Analyze a single document with the warmed up tagger sub-process.
   * Ensure all settings are made correctly and 
   * run any pre-processing steps if configured. */
  @Override
  public void execute() throws ExecutionException {
    if (document == null) throw new ExecutionException("no document to process.");

    if (encoding == null) throw new ExecutionException("no document encoding specified");

    if (regex == null || regex.trim().equals(""))
      throw new ExecutionException("no regular expression for processing tagger output provided");

    if (!featureMapping.containsKey(stringFeatureName))
      throw new ExecutionException(
              "The feature mapping must include an entry for stringFeatureName " +
              "in order to map between the tagger and the document/annotations");

    if (preProcess != null) {
      preProcess.setInputASName(inputASName);
      preProcess.setOutputASName(inputASName);
      preProcess.setDocument(document);

      try {
        preProcess.execute();
      } finally {
        preProcess.setDocument(null);
      }
    }

		process();

    if(postProcess != null) {
      postProcess.setInputASName(outputASName);
      postProcess.setOutputASName(outputASName);
      postProcess.setDocument(document);

      try {
        postProcess.execute();
      } finally {
        postProcess.setDocument(null);
      }
    }
  }

	/**
	 * Process the current GATE document.  
   * This method does all the work by sending input text to the tagger
   * and reading the output of the tagger, updating or creating
   * annotations on the document being processed. */ 
  protected void process() throws ExecutionException {
		CharsetEncoder charsetEncoder = charset.newEncoder().onUnmappableCharacter(
				failOnUnmappableCharacter ?
				CodingErrorAction.REPORT :
				CodingErrorAction.REPLACE);
		PrintWriter in = new PrintWriter(new OutputStreamWriter(input, charsetEncoder));

		CharsetDecoder charsetDecoder = charset.newDecoder();
		BufferedReader out = new BufferedReader(new InputStreamReader(output, charsetDecoder));

		AnnotationSet annotSet = (inputASName == null || inputASName.trim().equals("")) ?
			document.getAnnotations() :
			document.getAnnotations(inputASName);

		// filter the input annotation type configured for the tagger
		annotSet = annotSet.get(inputAnnotationType);

		if (annotSet == null || annotSet.size() == 0) {
			if (failOnMissingInputAnnotations) {
				throw new ExecutionException(
						"no " + inputAnnotationType + " found in document " + document.getName());
			} else {
				logger.info("No input annotations in document " + document.getName());
				System.err.println("No input annotations in document " + document.getName());
				return;
			}
		}

		// sort annotations according to their offsets
		List<Annotation> inputAnnotations = new ArrayList<Annotation>(annotSet);
		Collections.sort(inputAnnotations, new OffsetComparator());

		for(int i = 0; i < inputAnnotations.size(); i++) {
			// write the input string to the tagger's input stream
			in.println(makeTaggerInput(inputAnnotations.get(i)));
			// ensure the input isn't being buffered
			in.flush();
			// read the result from the tagger's output stream
			readOutput(out, inputAnnotations.get(i)); 
		}
  }

  /**
   * Returns the text that will be sent to the tagger for a given
   * annotation. This is the value of the configured string feature 
   * that is expected to be present on the annotation.
   * 
   * @param ann the annotation
   * @return the string to be passed to the tagger for this annotation
   */
  protected String makeTaggerInput(Annotation ann) throws ExecutionException {
    FeatureMap features = ann.getFeatures();

		if (features.containsKey(stringFeatureName)) {
			String input = (String) features.get(stringFeatureName);

      if (debug) {
      	logger.info("Tagger input:");
      	logger.info(input);
      	System.out.println(input);
			}

			return input;
		} else if (failOnMissingInputAnnotations) {
			throw new ExecutionException(
					"the input annotation has no feature named '" + stringFeatureName + "'");
		} else {
			if (debug) logger.info("Tagger input annotation has no feature.");
			return ""; // fallback: send the empty string
		}
  }

  /**
   * This method reads the output from the tagger, annotating the result
   * on the GATE document. The tagger is expected to produce one line of
   * output per annotation to be created or updated.
   *  
   * @param output stream from which to read the output from the tagger
   * @param inputAnnotation that was used to send the text to the tagger
   * @throws ExecutionException if an error occurs while handling the output from the tagger
   */
  protected void readOutput(BufferedReader output, Annotation inputAnnotation)
  	throws ExecutionException {
    AnnotationSet outSet = (outputASName == null || outputASName.trim().length() == 0) ?
    	document.getAnnotations() :
    	document.getAnnotations(outputASName);
		String inputContent = annotationContent(inputAnnotation);

		if (debug) logger.info("Tagger Output:");

    try {
      // only work with output annotations that occur within the input annotation range
      List<Annotation> outputAnnotations = new ArrayList<Annotation>(outSet.get(
      			outputAnnotationType,
      			inputAnnotation.getStartNode().getOffset(),
      			inputAnnotation.getEndNode().getOffset()));

      if (!updateAnnotations) outSet.removeAll(outputAnnotations);
      else Collections.sort(outputAnnotations, new OffsetComparator());

      String line;
      int currentPosition = 0;

      while((line = output.readLine()) != null) {
      	if (line.trim().length() == 0) {
      		if (debug) logger.info("Empty line ends block.");
      		break;
				}

        Matcher result = pattern.matcher(line);

        if (result.matches()) {
					if (debug) {
						System.out.println(line);
						logger.info(line);
					}

          FeatureMap features = Factory.newFeatureMap();

          for(Map.Entry<Object, Object> kv : featureMapping.entrySet()) {
            int groupNumber = Integer.parseInt(String.valueOf(kv.getValue()));

            // ignore feature mapping if there isn't a match for that group
            if(result.start(groupNumber) >= 0) {
              features.put(kv.getKey(), result.group(groupNumber));
            }
          }

					if (updateAnnotations) outputAnnotations = updateAnnotation(
							inputAnnotation, outSet, features, outputAnnotations);
					else currentPosition = createAnnotation(
							inputAnnotation, outSet, features, inputContent, currentPosition);
        } else if (debug) {
          System.err.println("Line didn't match input pattern:\n" + line);
        }
      }

			if (debug) logger.info("Output closed or block terminated.");
    } catch (Exception err) {
      err.printStackTrace();
      throw (ExecutionException)new ExecutionException(
              "Error occurred running tagger").initCause(err);
    }
  }

	/**
	 * Create a new annotation with the output features.
	 * The tagger output is contained in the outputFeatures, with a
	 * stringFeatureName key (runtime parameter) that stores the string that has
	 * to be matched with the input content.
	 *
	 * @param inputAnnotation the input annotation used to generate the tagger's input
	 * @param outSet the annotation set container to which the output annotation will be added
	 * @param outputFeatures the feature map generated from the tagger's output
	 * @param inputContent the properly encoded input string to match the output token against
	 * @param currentPosition offset after which to look for a match in inputContent
	 * @return the next offset position to start looking for matching content */
	private int createAnnotation(
			Annotation inputAnnotation,
			AnnotationSet outputSet,
			FeatureMap outputFeatures,
			String inputContent,
			int currentPosition) throws ExecutionException {
		String target = (String) outputFeatures.get(stringFeatureName);
		int nextPosition = -1;

		if (debug) 
			logger.info("matching '" + target + "' in '" + inputContent +
					"' at offset=" + currentPosition + "/" + inputContent.length());

		if ((nextPosition = inputContent.indexOf(target, currentPosition)) == -1) {
			throw new ExecutionException(
					"cannot find '" + target + "' in the input text after '" +
					inputContent.substring(currentPosition) + "'");
		}

		Long offset = inputAnnotation.getStartNode().getOffset();
		Long start = offset + nextPosition;
		Long end = start + target.length();
		
		try {
			outputSet.add(start, end, outputAnnotationType, outputFeatures);
		} catch (InvalidOffsetException ex) {
			throw new ExecutionException(ex);
		}

		if (debug) logger.info("new position will be at " + (end - offset));
		return (int) (end - offset); // new position
	}

	/**
	 * Update an existing annotation with the output features.
	 * The method will popule the output annotation "buffer" if it is empty with
	 * all output annotations within the current offsets of the input annotation.
	 * Then, it checks that the string feature on the output matches the string
	 * feature on the annotation to be updated. If all is well, the matching
	 * annotation is updated with the new features except the string feature
	 * itself.
	 * 
	 * @param inputAnnotation the input annotation used to generate the tagger's input
	 * @param outSet the container with the annotations that will be updated
	 * @param outputFeatures the feature map generated from the tagger's output
	 * @param outputAnnotations a "buffered" list of output annotations to update
	 * @return the updated list of output annotations to update (in sorted order) */
	private List<Annotation> updateAnnotation(
			Annotation inputAnnotation,
			AnnotationSet outSet,
			FeatureMap outputFeatures,
			List<Annotation> outputAnnotations) throws ExecutionException {
		if (outputAnnotations.size() == 0) {
			outputAnnotations.addAll(outSet.get(
						outputAnnotationType,
						inputAnnotation.getStartNode().getOffset(),
						inputAnnotation.getEndNode().getOffset()));
			Collections.sort(outputAnnotations, new OffsetComparator());
		}

		if (outputAnnotations.size() > 0) {
			Annotation toUpdate = outputAnnotations.remove(0);
			String raw = (String) toUpdate.getFeatures().get(stringFeatureName);

			try {
				String encoded = new String(charset.encode(raw).array(), encoding).trim();

				if (!encoded.equals(outputFeatures.get(stringFeatureName)))
					throw new ExecutionException("annotations are out of sync: " + encoded
									+ " != " + outputFeatures.get(stringFeatureName));
			} catch (UnsupportedEncodingException ex) {
				throw new ExecutionException(ex);
			}

			// remove the feature to maintain the existing one rather than the encoded version
			outputFeatures.remove(stringFeatureName);
			toUpdate.getFeatures().putAll(outputFeatures);
		} else {
			throw new ExecutionException("ran out of output annotations");
		}

		return outputAnnotations;
	}

	/** Get the properly encoded text covered by the given annotation. */
	private String annotationContent(Annotation ann) throws ExecutionException {
			try {
				return new String(charset.encode(document.getContent().getContent(
								ann.getStartNode().getOffset(),
								ann.getEndNode().getOffset()
						).toString()).array(), encoding).trim();
			} catch (UnsupportedEncodingException ex) {
				throw new ExecutionException(ex);
			} catch (InvalidOffsetException ex) {
				throw new ExecutionException(ex);
			}
	}

	// Initialization Parameters
	
  @CreoleParameter(comment = "name of the tagger command file")
  public void setTaggerBinary(URL taggerBinary) {
    this.taggerBinary = taggerBinary;
  }

  public URL getTaggerBinary() {
    return taggerBinary;
  }

  @Optional
  @CreoleParameter(comment = "directory in which to run the tagger")
  public void setTaggerDir(URL taggerDir) {
    this.taggerDir = taggerDir;
  }

  public URL getTaggerDir() {
    return taggerDir;
  }

  @CreoleParameter(defaultValue = "", comment = "flags passed to tagger script")
  public void setTaggerFlags(List<String> taggerFlags) {
    this.taggerFlags = taggerFlags;
  }

  public List<String> getTaggerFlags() {
    return taggerFlags;
  }

  @Optional
  @CreoleParameter(comment = "JAPE grammar to use for pre-processing")
  public void setPreProcessURL(URL preProcessURL) {
    this.preProcessURL = preProcessURL;
  }

  public URL getPreProcessURL() {
    return preProcessURL;
  }

  @Optional
  @CreoleParameter(comment = "JAPE grammar to use for post-processing")
  public void setPostProcessURL(URL postProcessURL) {
    this.postProcessURL = postProcessURL;
  }
  
  public URL getPostProcessURL() {
    return postProcessURL;
  }

	// Runtime Parameters
	
  @RunTime
  @CreoleParameter(defaultValue = "UTF-8", comment = "character encoding used by the tagger")
  public void setEncoding(String encoding) {
    this.encoding = encoding;
    this.charset = Charset.forName(encoding);
  }

  public String getEncoding() {
    return encoding;
  }

  @RunTime
  @CreoleParameter(defaultValue = "^([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)",
  		comment = "a regex to process tagger output with capture groups referenced by the 'featureMapping'")
  public void setRegex(String regex) {
    this.pattern = Pattern.compile(regex);
    this.regex = regex;
  }

  public String getRegex() {
    return regex;
  }

  @RunTime
  @CreoleParameter(defaultValue = "string=1;lemma=2;pos=3;chunk=4;entity=5",
  		comment = "mapping from feature names to matching groups in 'regex'; must include a 'stringFeatureName'")
  public void setFeatureMapping(FeatureMap featureMapping) {
    this.featureMapping = featureMapping;
  }

  public FeatureMap getFeatureMapping() {
    return featureMapping;
  }

  @RunTime
  @CreoleParameter(defaultValue = "string", comment = "name of the feature containing the text that will be sent to the tagger and the name of the resulting feature that will be matched to the document text to find the annotated region (see 'featureMapping')")
  public void setStringFeatureName(String name) {
    this.stringFeatureName = name;
  }

  public String getStringFeatureName() {
    return stringFeatureName;
  }

  @RunTime
  @CreoleParameter(defaultValue = "Sentence", comment = "annotation type used as input to tagger")
  public void setInputAnnotationType(String inputAnnotationType) {
    this.inputAnnotationType = inputAnnotationType;
  }

  public String getInputAnnotationType() {
    return inputAnnotationType;
  }

  @RunTime
  @CreoleParameter(defaultValue = "Token", comment = "annotation type created or updated by tagger")
  public void setOutputAnnotationType(String outputAnnotationType) {
    this.outputAnnotationType = outputAnnotationType;
  }

  public String getOutputAnnotationType() {
    return outputAnnotationType;
  }

  @RunTime
  @CreoleParameter(defaultValue = "false", comment = "update existing annotations or add new ones?")
  public void setUpdateAnnotations(Boolean updateAnnotations) {
    this.updateAnnotations = updateAnnotations;
  }

  public Boolean getUpdateAnnotations() {
    return updateAnnotations;
  }

  @RunTime
  @CreoleParameter(defaultValue = "false", comment = "turn on debugging output")
  public void setDebug(Boolean debug) {
    this.debug = debug;
  }

  public Boolean getDebug() {
    return debug;
  }

  @RunTime
  @CreoleParameter(defaultValue = "true", comment = "Should the tagger fail if it encounters a character which "
          + "is not mappable into the specified encoding?")
  public void setFailOnUnmappableCharacter(Boolean failOnUnmappableCharacter) {
    this.failOnUnmappableCharacter = failOnUnmappableCharacter;
  }

  public Boolean getFailOnUnmappableCharacter() {
    return failOnUnmappableCharacter;
  }

  @RunTime
  @CreoleParameter(defaultValue = "true", comment = "throw an exception when there are no input annotations")
  public void setFailOnMissingInputAnnotations(Boolean fail) {
    failOnMissingInputAnnotations = fail;
  }
  
  public Boolean getFailOnMissingInputAnnotations() {
    return failOnMissingInputAnnotations;
  }

  @Optional
  @RunTime
  @CreoleParameter(comment = "annotation set from which input annotations are taken")
  public void setInputASName(String inputASName) {
    this.inputASName = inputASName;
  }

  public String getInputASName() {
    return inputASName;
  }

  @Optional
  @RunTime
  @CreoleParameter(comment = "annotation set in which output annotations are created or updated")
  public void setOutputASName(String outputASName) {
    this.outputASName = outputASName;
  }

  public String getOutputASName() {
    return outputASName;
  }
}
