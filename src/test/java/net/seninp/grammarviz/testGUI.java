package net.seninp.grammarviz;

import net.seninp.gi.GIAlgorithm;
import net.seninp.gi.logic.GrammarRules;
import net.seninp.grammarviz.cluster.testCluster;
import net.seninp.grammarviz.controller.GrammarVizController;
import net.seninp.grammarviz.model.GrammarVizModel;
import net.seninp.grammarviz.view.GrammarVizView;
import net.seninp.jmotif.sax.NumerosityReductionStrategy;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import net.seninp.util.StackTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Main runnable of Sequitur GUI.
 *
 * @author psenin
 *
 */
public class testGUI {

    /** The model instance. */
    private static GrammarVizModel model;

    /** The controller instance. */
    private static GrammarVizController controller;

    /** The view instance. */
    private static GrammarVizView view;

    private static final String TEST_DATA_FNAME = "/Users/Jeremy/Desktop/毕设/参考文献/导师推荐/代码/grammarviz3-optimized/data/TEK17_SMOOTH.txt";
    private static final String SAVE_DATA_FNAME = "/Users/Jeremy/Desktop/毕设/参考文献/导师推荐/代码/grammarviz3-optimized/result_cluster_NONE.txt";

    private static final String LIMIT_STR = "";

    final static Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final String SPACE = " ";
    private static final String CR = "\n";

    private static final boolean USE_WINDOW_SLIDE = true;
    private static final int WIN_SIZE = 120;
    private static final int PAA_SIZE = 4;
    private static final int ALPHABET_SIZE = 8;
    private static final double NORM_THRESHOLD = 0.05;
    private static final int DISCORDS_TO_TEST = 5;
    private static final NumerosityReductionStrategy STRATEGY = NumerosityReductionStrategy.EXACT;

    private double[] series;

    private GrammarRules grammarRules;

    private static final Logger LOGGER = LoggerFactory.getLogger(testCluster.class);

    /**
     * Runnable GIU.
     *
     * @param args None used.
     */
    public static void main(String[] args) {

        System.out.println("Starting GrammarViz 3.0 ...");

        /** Boilerplate */
        // the locale setup
        Locale defaultLocale = Locale.getDefault();
        Locale newLocale = Locale.US;
        System.out.println(
                "Changing runtime locale setting from " + defaultLocale + " to " + newLocale + " ...");
        Locale.setDefault(newLocale);

        // this is the Apple UI fix
        System.setProperty("apple.laf.useScreenMenuBar", "true");
        System.setProperty("com.apple.mrj.application.apple.menu.about.name", "SAXSequitur");

        /** On the stage. */
        // model...
        model = new GrammarVizModel();

        // controller...
        controller = new GrammarVizController(model);

        // view...
        view = new GrammarVizView(controller);

        // make sure these two met...
        model.addObserver(view);
        controller.addObserver(view);

        // live!!!
        view.showGUI();

    }

}
