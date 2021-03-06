package org.systemsbiology.xtandem.peptide;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * org.systemsbiology.xtandem.peptide.PeptideBondDigesaterTest
 *
 * @author Steve Lewis
 * @date 5/20/2015
 */
public class PeptideBondDigesaterTest {
    public static PeptideBondDigesaterTest[] EMPTY_ARRAY = {};
    public static Class THIS_CLASS = PeptideBondDigesaterTest.class;

    public static final String TEST_PROTEIN =
            "NDLQCLSAVGRNIWHKQPRDTPLEISLVYNTSHGTTSLTGRRASTKYVPCVYINQHLFMASEGPKLWIIPLPDYLIKPFS\n" +
                    "EGIQMKKRDWRAGELFLGKIYAGDEPNSEMVTEQTTVEFEFGIHDIPITYKRAYNQSVGTLFSQTFYFGSIWFVVPPGND\n" +
                    "IWEQFFALRALLDAVYGGLPKLSPYSKAMWMAPVKGVLMSSFVDELESSMLVQGKIARGLDILSRHVVETLRNFRILEQR\n" +
                    "LVTNMSEEYVVPYLKMVLELDFENPLKSLIDQALDEVVEQPSKGSGGSQRPLTLLVGQFLQNTEQNDKTIDANEHLGFVE\n" +
                    "PHATIPLNRLYDIYSQYSGHPPIYYTDGPALFYHDQEIEKCYFTSLLSLLLRRDKDDTVRGGYNCEGTLYTLADFPVEKY\n" +
                    "DNLFMQIQRMSIRLDSENFEYPINWGLPGFNRREQVIAHFFCLGFLLKQWMVAKTCSQFFVPDSIPDNLYSRLLNARLGK\n" +
                    "PPENTMKIGNQLISVPFKESPYSTLWLRFGVNTSEPVIVEECIKELAPMWSTALHCNQLVVWTGDRIASNIMKAAIPGQG\n" +
                    "QGLSITQTKTGGMGLDDAFKLLGAMPDAGPSLVFILPACCNSDSYSGQLDFTPAEIYVSGMHEAIFGRIAPIMKDPRLCR\n" +
                    "LIVMRELGHLFKWSGPFTEEHPWASDYIFKWENGNQELHEMLGKLKPLASARVVEAWAKESLWESVPNPFPNDLAIGGTL\n" +
                    "LFYWIEENIQKKEKMIGITLLLSFLLKDKEFLSRCVNNYISLTFHDIIYEIRLELEESKKSHALSQMYLNIFWTLSYQYM\n" +
                    "PEIHALDSICFFITASHVAVPKYGMRTEDIQTETVSAIEQKESIEESLVKSSSLIKIATEDELINGESLSLVELIKDEIE\n" +
                    "KLQKKNKASEVILKNKKEELEPKEKAAVIGLLQDQLGLPTIMFNLLCVKVAVEPLYHPNRLRTTIYLKFDRSYEIINEGL\n" +
                    "RMYEVGQQKFTSKLLIPEIFADLEEGVNELLVPTGFQLAHELTRVYNTDSFKIVSLKNAKEMNKIWKNAQGQPDIMLAWR\n" +
                    "RSNSVIIGNDISFSDIPLGAIQWARIKVPDGLTNSLSFDSSGPIVKDKCQALWQKQCEARYDVTFAGLYAVTGSSLLVDG\n" +
                    "TLNTYRIGLQRAAETWRDKEGGLGSILKEARILKQSCIEINGELTKKKTNMEEFDDNLAQLRDEVLKLEARKQNLKQMQT\n" +
                    "DLKGEAEKLRERKPAVVKAVRDYVEMARVWKCLGECASSVNKIVAPQFDPHDIFRERIRKMIVPPINDKDYTKLSELFKL\n" +
                    "DGLIKRSVGWYDEIMKGSGSPDPKREPRLGKMVCISEMVLKVPGPPNQMAKVLSIDAPNLTDLAALAAELAPMAEALDGE\n" +
                    "CENKIGQAIAAAANAEKEDAQVLLKKADAEQTEAEIKVMMKATEESTQILQPQLATLEVQMVAVQSAAFELKQLGTLYRN\n" +
                    "RMMDVEQRKKNLLTKFTLILELYSTPTVYNHRRLTNYYDLSLKKVSEQFYKCMSIVQIRINDDLEVDELFKNAVLELADT\n" +
                    "PWSQFWDITCCNILSPFMRLRNRFADGIPSMALSFCIENTREGKEIFFNYMSLPTVEIKEGETRAAMQMKEVIDAKEDAP\n" +
                    "FINPVDGTNLLMNIDEVFSEDKIQNDAFLFVTSKTAVGVQLMIKKLDERWDNGSYSKTIEIQYLEYSNMFTSLKSASQRG\n" +
                    "SGGIGVLLLHGKDQKLVRCIRSIHEIAFRFMVLSMPAKSINNFEELYYEMVVTLQKLDTIEDYIKQDSEPKFFDGFFLSR\n" +
                    "INDDVIKGTPSLHILVKEVSQKFCNSTTERVMNFFVQRDEHDILRDYFVRYVEHIWLRIFKEVDQLHTHPCLLVGQIVRS\n" +
                    "FDRLNFVYHSKSPTPLFNEVAAKYITMTAQVMMKGYRLFVVDFGKGFHWDAISSFIKTLIDDEFANISIINLHRTFRGTI\n" +
                    "DNRGGGPPGMATVLLVDVIDLKNTDKKDFWYGHDIWQRLLEIPPQAGYVEKAPMNLDDVFVVAKKGIPPGFLGKRRRDLK\n" +
                    "SMIIDQTQNASTRASFNICNPLYTNKPLRLLFNNTIVSKGTGTPGVFLMPIEHDLYTKLFFSQRATEMTPIILESVKASA\n" +
                    "SIHEEEKTIYETWTYWHGSAQKLFYFDYISGREPFINNKTLKVSKPRPNNNDMGMILNRFFLDFKKRSDANITGAVTWVL\n" +
                    "AFLFLGQLWLFIQQSTLGEFLESEEEEVERIEDLLSSYLRMMSFALHIPSTQVVFKCHLRTFELCPQVLWMFMDNVLEKH\n" +
                    "EETLSSPLTDMYSDKLPKWGLQHPEMYIMGCRSVTAPSAQELDAPEFILSMKPSMQIIEGSMLCLKKNDDLVTNMNEIWV\n" +
                    "ADVPGDFIIWKRDDSLSSAQERFANALVGDTWEHSVQDFCGYLQGMTIAKPNIIKYEVAFEEMQKAEYLDGLAAALVKYA\n" +
                    "CTKGGMPDGVIMYGHRVLMMEYIQIIKGIFWPVPQLKMKKINANLVEMFVVYDPQPLIVGPFLDSIIGQFLPVDQALFKA\n" +
                    "LNVDLLARLLLVSENEEPYKLKLNGAATLVSKVARMGYDYHHQSSLQESCLRYTAVIKQALSRSDLFGMSYLSIEGILAY\n" +
                    "DPVMMAVTRFLAKLNDPLEARGAYGPNMTIFVACTPNLSLETGEFIFTKLKRIIAQQISLIQQAVVSLVEVEIRNFEDFC\n" +
                    "AWAGAQALGKFFKGMAKYDLGDSCNFVVCQKALAKALDKTTETKGTGAPGEPAGGLNLKLAGMLTRYCRDTLPTIVLRPS\n" +
                    "NGLYEYGYLAETTIMQVQVDKSDWYYRLQSIWQFDNLDCVNDESLKAVVDRAHVDIVTLAGLTLRAGSSLKGRVLQVIQA\n" +
                    "IQDNSKKLFDPLTKEILAQSVEQTWFISSVCIVVQGPWQLVWQNRPVQVYAEIGLKIVEQMSALMMQEVQLLWKEVMGKA\n" +
                    "QAPYIKQKFPIIEKESSIMGVIELSDTFELKAIGEFCKKLHPQVRLPDKTESLIELLEDNSLFFFRPFFLRKKELYDNLG\n" +
                    "KQIDELLLNAEQLREAMRPQDAAMLVRTDKVAQSMLSKWYNDVIAFKRGEEPMQAIIDESSFIPELYLWTAQCKLWADLI\n" +
                    "EQVRVLKEEWKRCETEIPKVFPSGCMTQTKIVHDDLLLQIDDVACLISTDTDRYKVFNFTMNVWDLKMKELNKELSYEKS\n" +
                    "AAAGIPELKEIFKGFGFELMNSLCTTETPKIEYGVIESIQQWHRDKMGPNCSITLIPIHQKFKDIKIKVNEALRRPAPMD\n" +
                    "ALTKTLKYITRWMNGIEEAIEEANLLFLPGNMWEESKTSFEYATVWLQEYPVKNTMVIQLLPFTSKEKELLEEEKNILEF\n" +
                    "EVLAQDLNKSLENLKEVNNKMEETTMVERKRFGELEKSYGELKVEFESCRKILDMEAQDRKSLLLNRSNEFIDEIQHPLD\n" +
                    "AYDMLFELREVADRLQRRLKFVTVDSSKKLFEILFVLEGTNIPIESVKDAILSYQNCISTNTDRNVDVQFQILRDKLNQA\n" +
                    "RECLDYNLTMTDLCFMALPVTIHMSAIENRRKKISNIRMVFDEIEHNEKLFESINQEATNDLLDDYKKYVNLYKCPGVQN\n" +
                    "KQFVEFTQDVFDSVLKEDANVTGLILNYGKLEPFLISEVQQLWVISLTQFCLYSHFPKPCCSKPIGKSNKIIELFSNRIL\n" +
                    "EWCDDFSPNFVIIPENVELKIMILQPIFFEMEQYPEEFDNGDKHIMFFSVLDKLSNIVLERLQLSMFSAVSAFYEEINRS\n" +
                    "SDYDSKPAFHIWQEKRSVFLQACTPIWKNLLIQRAEMCHKQIVDQYEHPQLPLKGALLEATRVFRLDRFESFWLQKLSLM\n" +
                    "MPNVTHLHEENWKKANRYVSHWPVPARIVRQPFARPTSQIFLRKREMPDMLIYDVISKMLSSYYDTEKEEVLAAMLPELF\n" +
                    "PSTLLTNSILKSIRVMVKDEEPAIMDKRIGSTLYYYYRELDSESPKTEKSEIEREQQLMVDIQEEPSMPRSGPFTLSTVV\n" +
                    "EKKMPLSTSASKMGTLDEKDKPSSKMKKKNPESSSKNALVSPLGQVIKERPVSITVPQYVKMLDKLKLDRATLSYNNAIS\n" +
                    "DSPGSHHHQEKFPAALTWSTRQMLPPYFSHSMVTKYLESPEENASAPLPPLEPRMQSHSMHYISDSKPIKQSSCDTDNM";


    public static final String[] EXPECTED_PEPTIDE_STRINGS =
            {
                    "YITMTAQVMMKGYR"
            };


    @Test
    public void testDigest() throws Exception {
        IProtein prot = Protein.buildProtein("F1MRU4_REVERSED",
                "tr|F1MRU4_REVERSED|F1MRU4_BOVIN Uncharacterized protein OS=Bos taurus GN=DNAH3 PE=4 SV=2-REVERSED",
                TEST_PROTEIN, "xx");

        PeptideBondDigester digester = PeptideBondDigester.getDigester(PeptideBondDigester.TRYPSIN_LOGIC);
        digester.setNumberMissedCleavages(2);

        IPolypeptide[] digest = PeptideBondDigester.TRYPSIN2.digest(prot);
        List<IPolypeptide> digesteList = new ArrayList<IPolypeptide>(Arrays.asList(digest));
            for (int i = 0; i < EXPECTED_PEPTIDE_STRINGS.length; i++) {
                    IPolypeptide test = Polypeptide.fromString(EXPECTED_PEPTIDE_STRINGS[i]);
                    Assert.assertTrue(digesteList.contains(test));

            }
    }
}
