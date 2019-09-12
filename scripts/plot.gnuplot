set term pdf dashed size 5,3 #png size 1024,800 enhanced font "Arial" 20;

set bmargin 3;
set lmargin 11;
set tmargin 2;
set ytics autofreq font "Helvetica,24";
set xlabel font "Helvetica,25";
set ylabel font "Helvetica,25";
set autoscale x;
set autoscale y;

unset key
set datafile separator " "
set xlabel "Time (s)" offset 0,0.25;
set ylabel "MB/s" offset -2.0,0.0;
set output filename2;
#set xrange[0:300];
if(iops == 0) {
   set yrange[0:2700];
} else {
   set yrange[0:500000];
}
set linetype 1 lc rgb 'black'

back1 = 0
back2 = 0
back3 = 0
back4 = 0
back5 = 0
back6 = 0
back7 = 0
back8 = 0
back9 = 0
back10 = 0
back11 = 0
back12 = 0
back13 = 0
back14 = 0
back15 = 0
back16 = 0
back17 = 0
back18 = 0
back19 = 0
back20 = 0
back21 = 0
back22 = 0
back23 = 0
back24 = 0
back25 = 0
back26 = 0
back27 = 0
back28 = 0
back29 = 0
back30 = 0
back31 = 0
back32 = 0
back33 = 0
back34 = 0
back35 = 0
back36 = 0
back37 = 0
back38 = 0
back39 = 0
back40 = 0
back41 = 0
back42 = 0
back43 = 0
back44 = 0
back45 = 0
back46 = 0
back47 = 0
back48 = 0
back49 = 0
back50 = 0
back51 = 0
back52 = 0
back53 = 0
back54 = 0
back55 = 0
back56 = 0
back57 = 0
back58 = 0
back59 = 0
back60 = 0
back61 = 0
back62 = 0
back63 = 0
back64 = 0
back65 = 0
back66 = 0
back67 = 0
back68 = 0
back69 = 0
back70 = 0
back71 = 0
back72 = 0
back73 = 0
back74 = 0
back75 = 0
back76 = 0
back77 = 0
back78 = 0
back79 = 0
back80 = 0
back81 = 0
back82 = 0
back83 = 0
back84 = 0
back85 = 0
back86 = 0
back87 = 0
back88 = 0
back89 = 0
back90 = 0
back91 = 0
back92 = 0
back93 = 0
back94 = 0
back95 = 0
back96 = 0
back97 = 0
back98 = 0
back99 = 0
back100 = 0
back101 = 0
back102 = 0
back103 = 0
back104 = 0
back105 = 0
back106 = 0
back107 = 0
back108 = 0
back109 = 0
back110 = 0
back111 = 0
back112 = 0
back113 = 0
back114 = 0
back115 = 0
back116 = 0
back117 = 0
back118 = 0
back119 = 0
back120 = 0
back121 = 0
back122 = 0
back123 = 0
back124 = 0
back125 = 0
back126 = 0
back127 = 0
back128 = 0
back129 = 0
back130 = 0
back131 = 0
back132 = 0
back133 = 0
back134 = 0
back135 = 0
back136 = 0
back137 = 0
back138 = 0
back139 = 0
back140 = 0
back141 = 0
back142 = 0
back143 = 0
back144 = 0
back145 = 0
back146 = 0
back147 = 0
back148 = 0
back149 = 0
back150 = 0
back151 = 0
back152 = 0
back153 = 0
back154 = 0
back155 = 0
back156 = 0
back157 = 0
back158 = 0
back159 = 0
back160 = 0
back161 = 0
back162 = 0
back163 = 0
back164 = 0
back165 = 0
back166 = 0
back167 = 0
back168 = 0
back169 = 0
back170 = 0
back171 = 0
back172 = 0
back173 = 0
back174 = 0
back175 = 0
back176 = 0
back177 = 0
back178 = 0
back179 = 0
back180 = 0
back181 = 0
back182 = 0
back183 = 0
back184 = 0
back185 = 0
back186 = 0
back187 = 0
back188 = 0
back189 = 0
back190 = 0
back191 = 0
back192 = 0
back193 = 0
back194 = 0
back195 = 0
back196 = 0
back197 = 0
back198 = 0
back199 = 0
back200 = 0
samples(x) = $0 > 200 ? 200 : ($0+1)
avg5(x) = (shift5(x), (back1+back2+back3+back4+back5+back6+back7+back8+back9+back10+back11+back12+back13+back14+back15+back16+back17+back18+back19+back20+back21+back22+back23+back24+back25+back26+back27+back28+back29+back30+back31+back32+back33+back34+back35+back36+back37+back38+back39+back40+back41+back42+back43+back44+back45+back46+back47+back48+back49+back50+back51+back52+back53+back54+back55+back56+back57+back58+back59+back60+back61+back62+back63+back64+back65+back66+back67+back68+back69+back70+back71+back72+back73+back74+back75+back76+back77+back78+back79+back80+back81+back82+back83+back84+back85+back86+back87+back88+back89+back90+back91+back92+back93+back94+back95+back96+back97+back98+back99+back100+back101+back102+back103+back104+back105+back106+back107+back108+back109+back110+back111+back112+back113+back114+back115+back116+back117+back118+back119+back120+back121+back122+back123+back124+back125+back126+back127+back128+back129+back130+back131+back132+back133+back134+back135+back136+back137+back138+back139+back140+back141+back142+back143+back144+back145+back146+back147+back148+back149+back150+back151+back152+back153+back154+back155+back156+back157+back158+back159+back160+back161+back162+back163+back164+back165+back166+back167+back168+back169+back170+back171+back172+back173+back174+back175+back176+back177+back178+back179+back180+back181+back182+back183+back184+back185+back186+back187+back188+back189+back190+back191+back192+back193+back194+back195+back196+back197+back198+back199+back200)/samples(x))
shift5(x) = (back200 = back199, back199 = back198, back198 = back197, back197 = back196, back196 = back195, back195 = back194, back194 = back193, back193 = back192, back192 = back191, back191 = back190, back190 = back189, back189 = back188, back188 = back187, back187 = back186, back186 = back185, back185 = back184, back184 = back183, back183 = back182, back182 = back181, back181 = back180, back180 = back179, back179 = back178, back178 = back177, back177 = back176, back176 = back175, back175 = back174, back174 = back173, back173 = back172, back172 = back171, back171 = back170, back170 = back169, back169 = back168, back168 = back167, back167 = back166, back166 = back165, back165 = back164, back164 = back163, back163 = back162, back162 = back161, back161 = back160, back160 = back159, back159 = back158, back158 = back157, back157 = back156, back156 = back155, back155 = back154, back154 = back153, back153 = back152, back152 = back151, back151 = back150, back150 = back149, back149 = back148, back148 = back147, back147 = back146, back146 = back145, back145 = back144, back144 = back143, back143 = back142, back142 = back141, back141 = back140, back140 = back139, back139 = back138, back138 = back137, back137 = back136, back136 = back135, back135 = back134, back134 = back133, back133 = back132, back132 = back131, back131 = back130, back130 = back129, back129 = back128, back128 = back127, back127 = back126, back126 = back125, back125 = back124, back124 = back123, back123 = back122, back122 = back121, back121 = back120, back120 = back119, back119 = back118, back118 = back117, back117 = back116, back116 = back115, back115 = back114, back114 = back113, back113 = back112, back112 = back111, back111 = back110, back110 = back109, back109 = back108, back108 = back107, back107 = back106, back106 = back105, back105 = back104, back104 = back103, back103 = back102, back102 = back101, back101 = back100, back100 = back99, back99 = back98, back98 = back97, back97 = back96, back96 = back95, back95 = back94, back94 = back93, back93 = back92, back92 = back91, back91 = back90, back90 = back89, back89 = back88, back88 = back87, back87 = back86, back86 = back85, back85 = back84, back84 = back83, back83 = back82, back82 = back81, back81 = back80, back80 = back79, back79 = back78, back78 = back77, back77 = back76, back76 = back75, back75 = back74, back74 = back73, back73 = back72, back72 = back71, back71 = back70, back70 = back69, back69 = back68, back68 = back67, back67 = back66, back66 = back65, back65 = back64, back64 = back63, back63 = back62, back62 = back61, back61 = back60, back60 = back59, back59 = back58, back58 = back57, back57 = back56, back56 = back55, back55 = back54, back54 = back53, back53 = back52, back52 = back51, back51 = back50, back50 = back49, back49 = back48, back48 = back47, back47 = back46, back46 = back45, back45 = back44, back44 = back43, back43 = back42, back42 = back41, back41 = back40, back40 = back39, back39 = back38, back38 = back37, back37 = back36, back36 = back35, back35 = back34, back34 = back33, back33 = back32, back32 = back31, back31 = back30, back30 = back29, back29 = back28, back28 = back27, back27 = back26, back26 = back25, back25 = back24, back24 = back23, back23 = back22, back22 = back21, back21 = back20, back20 = back19, back19 = back18, back18 = back17, back17 = back16, back16 = back15, back15 = back14, back14 = back13, back13 = back12, back12 = back11, back11 = back10, back10 = back9, back9 = back8, back8 = back7, back7 = back6, back6 = back5, back5 = back4, back4 = back3, back3 = back2, back2 = back1, back1 = x)

#
# Initialize a running sum
#
init(x) = (back1 = back2 = back3 = back4 = back5 = back6 = back7 = back8 = back9 = back10 = back11 = back12 = back13 = back14 = back15 = back16 = back17 = back18 = back19 = back20 = back21 = back22 = back23 = back24 = back25 = back26 = back27 = back28 = back29 = back30 = back31 = back32 = back33 = back34 = back35 = back36 = back37 = back38 = back39 = back40 = back41 = back42 = back43 = back44 = back45 = back46 = back47 = back48 = back49 = back50 = back51 = back52 = back53 = back54 = back55 = back56 = back57 = back58 = back59 = back60 = back61 = back62 = back63 = back64 = back65 = back66 = back67 = back68 = back69 = back70 = back71 = back72 = back73 = back74 = back75 = back76 = back77 = back78 = back79 = back80 = back81 = back82 = back83 = back84 = back85 = back86 = back87 = back88 = back89 = back90 = back91 = back92 = back93 = back94 = back95 = back96 = back97 = back98 = back99 = back100 = back101 = back102 = back103 = back104 = back105 = back106 = back107 = back108 = back109 = back110 = back111 = back112 = back113 = back114 = back115 = back116 = back117 = back118 = back119 = back120 = back121 = back122 = back123 = back124 = back125 = back126 = back127 = back128 = back129 = back130 = back131 = back132 = back133 = back134 = back135 = back136 = back137 = back138 = back139 = back140 = back141 = back142 = back143 = back144 = back145 = back146 = back147 = back148 = back149 = back150 = back151 = back152 = back153 = back154 = back155 = back156 = back157 = back158 = back159 = back160 = back161 = back162 = back163 = back164 = back165 = back166 = back167 = back168 = back169 = back170 = back171 = back172 = back173 = back174 = back175 = back176 = back177 = back178 = back179 = back180 = back181 = back182 = back183 = back184 = back185 = back186 = back187 = back188 = back189 = back190 = back191 = back192 = back193 = back194 = back195 = back196 = back197 = back198 = back199 = back200 = sum = 0)

plot filename1 using 1:2 with lines, \
     '' using 1:(avg5($2)) with lines lw 3 lc rgb "red"

